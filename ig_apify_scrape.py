from __future__ import annotations

import argparse
import csv
import html
import json
import os
import re
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from dotenv import load_dotenv


@dataclass(frozen=True)
class Target:
    profile_url: str
    label: str
    start: date
    end: date  # end-exclusive


@dataclass(frozen=True)
class Config:
    apify_token: str
    apify_base_url: str
    output_dir: Path
    db_path: Path
    results_limit_per_type: int
    refresh_mode: str  # auto | always | never
    check_limit: int
    analysis_mode: str  # llm | none
    ollama_base_url: str
    ollama_model: str
    apify_poll_interval_seconds: float = 2.0
    apify_wait_timeout_seconds: int = 900


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_dt(value: str) -> datetime:
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _normalize_profile_url(url: str) -> str:
    u = url.strip()
    if not u:
        return u
    if not u.startswith("http"):
        u = "https://" + u.lstrip("/")
    if not u.endswith("/"):
        u += "/"
    return u


def _infer_handle(profile_url: str) -> str:
    # https://www.instagram.com/<handle>/
    try:
        parts = profile_url.rstrip("/").split("/")
        return parts[-1]
    except Exception:
        return profile_url


def _ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _connect_db(db_path: Path) -> sqlite3.Connection:
    _ensure_parent_dir(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def _init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS items (
            key TEXT PRIMARY KEY,
            profile_url TEXT NOT NULL,
            profile_label TEXT NOT NULL,
            profile_handle TEXT NOT NULL,
            content_kind TEXT NOT NULL, -- post|reel|unknown
            url TEXT,
            short_code TEXT,
            media_type TEXT,
            timestamp_utc TEXT,
            likes_count INTEGER,
            comments_count INTEGER,
            views_count INTEGER,
            caption TEXT,
            raw_json TEXT,
            inserted_at_utc TEXT NOT NULL,
            updated_at_utc TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS profile_period_cache (
            profile_url TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            completed_at_utc TEXT,
            items_inserted INTEGER NOT NULL DEFAULT 0,
            last_checked_at_utc TEXT,
            last_remote_latest_ts_utc TEXT,
            last_local_max_ts_utc TEXT,
            PRIMARY KEY (profile_url, start_date, end_date)
        )
        """
    )
    _ensure_columns(conn)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_items_profile_ts ON items(profile_url, timestamp_utc)")
    conn.commit()


def _ensure_columns(conn: sqlite3.Connection) -> None:
    """
    Лёгкая миграция SQLite (на случай существующей базы без новых колонок).
    """

    def cols(table: str) -> set[str]:
        return {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}

    items_cols = cols("items")
    if "updated_at_utc" not in items_cols:
        conn.execute("ALTER TABLE items ADD COLUMN updated_at_utc TEXT")

    cache_cols = cols("profile_period_cache")
    for col_def in [
        "last_checked_at_utc TEXT",
        "last_remote_latest_ts_utc TEXT",
        "last_local_max_ts_utc TEXT",
    ]:
        name = col_def.split()[0]
        if name not in cache_cols:
            conn.execute(f"ALTER TABLE profile_period_cache ADD COLUMN {col_def}")

    conn.commit()


def _month_window_utc(d: date) -> tuple[date, date]:
    start = d.replace(day=1)
    if start.month == 12:
        end = date(start.year + 1, 1, 1)
    else:
        end = date(start.year, start.month + 1, 1)
    return start, end


def _resolve_period(args: argparse.Namespace) -> tuple[date, date]:
    """
    Возвращает (start, end) как даты (UTC), где end — end-exclusive.
    Приоритет:
      1) --start/--end
      2) --period (this-month / last-month / last-30d)
      3) default: this-month
    """
    env_start = (os.environ.get("START_DATE") or "").strip() or None
    env_end = (os.environ.get("END_DATE") or "").strip() or None
    env_period = (os.environ.get("PERIOD") or "").strip() or None

    if args.start:
        start = date.fromisoformat(args.start)
        if args.end:
            end = date.fromisoformat(args.end)
        else:
            end = (datetime.now(timezone.utc).date() + timedelta(days=1))
        return start, end

    if args.end:
        end = date.fromisoformat(args.end)
        start, _ = _month_window_utc(end - timedelta(days=1))
        return start, end

    if env_start:
        start = date.fromisoformat(env_start)
        end = date.fromisoformat(env_end) if env_end else (datetime.now(timezone.utc).date() + timedelta(days=1))
        return start, end

    today = datetime.now(timezone.utc).date()
    period = (args.period or env_period or "this-month").strip().lower()
    if period == "this-month":
        return _month_window_utc(today)
    if period == "last-month":
        first_this, _ = _month_window_utc(today)
        prev_last_day = first_this - timedelta(days=1)
        return _month_window_utc(prev_last_day)
    if period == "last-30d":
        return today - timedelta(days=30), today + timedelta(days=1)
    raise SystemExit("Неизвестный --period. Используй: this-month | last-month | last-30d")


def _apify_request_json(
    *,
    base_url: str,
    token: str,
    method: str,
    path: str,
    params: Optional[dict[str, Any]] = None,
    body: Optional[dict[str, Any]] = None,
    timeout: int = 60,
) -> dict[str, Any]:
    q = dict(params or {})
    q["token"] = token
    url = base_url.rstrip("/") + "/" + path.lstrip("/")
    url = url + "?" + urllib.parse.urlencode(q, doseq=True)

    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json; charset=utf-8"

    req = urllib.request.Request(url=url, data=data, method=method.upper(), headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
        raise RuntimeError(f"Apify HTTP {e.code} {e.reason}: {raw}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Apify network error: {e}") from e


def _apify_run_actor_wait(*, cfg: Config, actor_id: str, run_input: dict[str, Any]) -> dict[str, Any]:
    started = _apify_request_json(
        base_url=cfg.apify_base_url,
        token=cfg.apify_token,
        method="POST",
        path=f"v2/acts/{actor_id}/runs",
        body=run_input,
        timeout=60,
    )
    run = started.get("data") or {}
    run_id = run.get("id")
    if not run_id:
        raise RuntimeError(f"Apify: не получил run id: {started}")

    deadline = time.time() + cfg.apify_wait_timeout_seconds
    while True:
        info = _apify_request_json(
            base_url=cfg.apify_base_url,
            token=cfg.apify_token,
            method="GET",
            path=f"v2/actor-runs/{run_id}",
            timeout=60,
        )
        data = info.get("data") or {}
        status = (data.get("status") or "").upper()
        if status in {"SUCCEEDED"}:
            return data
        if status in {"FAILED", "ABORTED", "TIMED-OUT"}:
            raise RuntimeError(f"Apify run {run_id} finished with status={status}: {json.dumps(data, ensure_ascii=False)}")
        if time.time() > deadline:
            raise RuntimeError(f"Apify run {run_id} did not finish within {cfg.apify_wait_timeout_seconds}s (status={status})")
        time.sleep(cfg.apify_poll_interval_seconds)


def _apify_iter_dataset_items(*, cfg: Config, dataset_id: str, page_limit: int = 1000) -> Iterable[dict[str, Any]]:
    offset = 0
    while True:
        q = {
            "token": cfg.apify_token,
            "format": "json",
            "clean": "true",
            "offset": offset,
            "limit": page_limit,
        }
        url = cfg.apify_base_url.rstrip("/") + f"/v2/datasets/{dataset_id}/items?" + urllib.parse.urlencode(q)
        req = urllib.request.Request(url=url, method="GET", headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=120) as r:
            raw = r.read().decode("utf-8")
            items = json.loads(raw) if raw else []
        if not items:
            return
        for it in items:
            yield it
        offset += len(items)


def _run_instagram_scraper(profile_url: str, results_type: str, results_limit: int, only_newer_than_utc: str, cfg: Config) -> dict[str, Any]:
    # Schema: https://apify.com/apify/instagram-scraper/input-schema
    actor_input: dict[str, Any] = {
        "directUrls": [profile_url],
        "resultsType": results_type,  # posts | reels
        "resultsLimit": results_limit,
        "onlyPostsNewerThan": only_newer_than_utc,  # YYYY-MM-DD (UTC)
        "addParentData": False,
    }
    return _apify_run_actor_wait(cfg=cfg, actor_id="apify~instagram-scraper", run_input=actor_input)


def _content_kind_from_url(url: Optional[str]) -> str:
    if not url:
        return "unknown"
    u = url.lower()
    if "/reel/" in u:
        return "reel"
    if "/p/" in u:
        return "post"
    return "unknown"


def _item_key(raw: dict[str, Any]) -> str:
    u = (raw.get("url") or "").strip()
    if u:
        return u
    sc = (raw.get("shortCode") or "").strip()
    if sc:
        return sc
    i = (raw.get("id") or "").strip()
    if i:
        return i
    return repr(raw)


def _to_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    try:
        return int(float(s))
    except ValueError:
        return None


def load_targets(path: Path, default_start: Optional[date], default_end: Optional[date]) -> list[Target]:
    if not path.exists():
        raise SystemExit(f"Не найден файл targets: {path}")

    out: list[Target] = []

    if path.suffix.lower() in {".txt", ".list"}:
        if not default_start or not default_end:
            raise SystemExit("Для targets.txt нужно задать период через --start/--end или --period.")
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                profile_url = _normalize_profile_url(s)
                label = _infer_handle(profile_url)
                out.append(Target(profile_url=profile_url, label=label, start=default_start, end=default_end))
    else:
        with path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames or "profile_url" not in reader.fieldnames:
                raise SystemExit("targets.csv должен иметь колонку 'profile_url' (и желательно label,start,end)")

            for row in reader:
                raw_url = (row.get("profile_url") or "").strip()
                if not raw_url:
                    continue
                profile_url = _normalize_profile_url(raw_url)
                label = (row.get("label") or "").strip() or _infer_handle(profile_url)

                start_s = (row.get("start") or "").strip()
                end_s = (row.get("end") or "").strip()

                start = date.fromisoformat(start_s) if start_s else default_start
                end = date.fromisoformat(end_s) if end_s else default_end
                if not start or not end:
                    raise SystemExit(
                        f"Для {profile_url} не задан период. "
                        "Укажи start/end в targets.csv или передай --start/--end/--period."
                    )
                if end <= start:
                    raise SystemExit(f"Некорректный период для {profile_url}: end <= start ({start}..{end})")

                out.append(Target(profile_url=profile_url, label=label, start=start, end=end))

    dedup: dict[tuple[str, date, date], Target] = {}
    for t in out:
        dedup[(t.profile_url, t.start, t.end)] = t
    return list(dedup.values())


def is_cached_complete(conn: sqlite3.Connection, profile_url: str, start: date, end: date) -> bool:
    row = conn.execute(
        """
        SELECT completed_at_utc
        FROM profile_period_cache
        WHERE profile_url = ? AND start_date = ? AND end_date = ? AND completed_at_utc IS NOT NULL
        """,
        (profile_url, start.isoformat(), end.isoformat()),
    ).fetchone()
    return bool(row and row[0])


def _local_max_ts(conn: sqlite3.Connection, profile_url: str, start: date, end: date) -> Optional[datetime]:
    row = conn.execute(
        """
        SELECT MAX(timestamp_utc)
        FROM items
        WHERE profile_url = ?
          AND timestamp_utc >= ? AND timestamp_utc < ?
        """,
        (profile_url, f"{start.isoformat()}T00:00:00+00:00", f"{end.isoformat()}T00:00:00+00:00"),
    ).fetchone()
    if not row or not row[0]:
        return None
    try:
        return _parse_iso_dt(str(row[0]))
    except Exception:
        return None


def _remote_latest_ts(cfg: Config, profile_url: str, start: date, check_limit: int) -> Optional[datetime]:
    """
    Дешёвая проверка: запускаем actor с маленьким resultsLimit и берём max(timestamp)
    среди полученных элементов (posts + reels).
    """
    only_newer_than = start.isoformat()
    best: Optional[datetime] = None
    for results_type in ("posts", "reels"):
        run = _run_instagram_scraper(
            profile_url=profile_url,
            results_type=results_type,
            results_limit=max(1, int(check_limit)),
            only_newer_than_utc=only_newer_than,
            cfg=cfg,
        )
        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            continue
        for raw in _apify_iter_dataset_items(cfg=cfg, dataset_id=str(dataset_id), page_limit=max(1, int(check_limit))):
            ts_raw = raw.get("timestamp")
            if not ts_raw:
                continue
            try:
                ts = _parse_iso_dt(ts_raw)
            except Exception:
                continue
            if best is None or ts > best:
                best = ts
    return best


def _mark_checked(
    conn: sqlite3.Connection,
    profile_url: str,
    start: date,
    end: date,
    remote_latest: Optional[datetime],
    local_max: Optional[datetime],
) -> None:
    now = _utc_now().isoformat()
    conn.execute(
        """
        INSERT INTO profile_period_cache(profile_url, start_date, end_date, last_checked_at_utc, last_remote_latest_ts_utc, last_local_max_ts_utc)
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(profile_url, start_date, end_date)
        DO UPDATE SET last_checked_at_utc = excluded.last_checked_at_utc,
                     last_remote_latest_ts_utc = excluded.last_remote_latest_ts_utc,
                     last_local_max_ts_utc = excluded.last_local_max_ts_utc
        """,
        (
            profile_url,
            start.isoformat(),
            end.isoformat(),
            now,
            (remote_latest.isoformat() if remote_latest else None),
            (local_max.isoformat() if local_max else None),
        ),
    )
    conn.commit()


def mark_cached_complete(conn: sqlite3.Connection, profile_url: str, start: date, end: date, inserted: int) -> None:
    now = _utc_now().isoformat()
    conn.execute(
        """
        INSERT INTO profile_period_cache(profile_url, start_date, end_date, completed_at_utc, items_inserted)
        VALUES(?, ?, ?, ?, ?)
        ON CONFLICT(profile_url, start_date, end_date)
        DO UPDATE SET completed_at_utc = excluded.completed_at_utc,
                     items_inserted = excluded.items_inserted
        """,
        (profile_url, start.isoformat(), end.isoformat(), now, int(inserted)),
    )
    conn.commit()


def scrape_one_target(cfg: Config, conn: sqlite3.Connection, target: Target) -> dict[str, Any]:
    local_max = _local_max_ts(conn, target.profile_url, target.start, target.end)

    if cfg.refresh_mode == "never":
        if is_cached_complete(conn, target.profile_url, target.start, target.end):
            return {"profile_url": target.profile_url, "label": target.label, "skipped": True, "reason": "cached", "inserted": 0}

    if cfg.refresh_mode == "auto":
        if local_max is not None and is_cached_complete(conn, target.profile_url, target.start, target.end):
            remote_latest = _remote_latest_ts(cfg, target.profile_url, target.start, cfg.check_limit)
            _mark_checked(conn, target.profile_url, target.start, target.end, remote_latest=remote_latest, local_max=local_max)
            if remote_latest is None or remote_latest <= local_max:
                return {
                    "profile_url": target.profile_url,
                    "label": target.label,
                    "skipped": True,
                    "reason": "no_new_posts",
                    "local_max_ts_utc": (local_max.isoformat() if local_max else None),
                    "remote_latest_ts_utc": (remote_latest.isoformat() if remote_latest else None),
                    "inserted": 0,
                }

    if cfg.refresh_mode == "always":
        remote_latest = _remote_latest_ts(cfg, target.profile_url, target.start, cfg.check_limit)
        _mark_checked(conn, target.profile_url, target.start, target.end, remote_latest=remote_latest, local_max=local_max)

    start_dt = datetime(target.start.year, target.start.month, target.start.day, tzinfo=timezone.utc)
    end_dt = datetime(target.end.year, target.end.month, target.end.day, tzinfo=timezone.utc)
    only_newer_than = target.start.isoformat()

    inserted_total = 0
    updated_total = 0
    fetched_total = 0

    for results_type in ("posts", "reels"):
        run = _run_instagram_scraper(
            profile_url=target.profile_url,
            results_type=results_type,
            results_limit=cfg.results_limit_per_type,
            only_newer_than_utc=only_newer_than,
            cfg=cfg,
        )
        dataset_id = run.get("defaultDatasetId")
        if not dataset_id:
            raise RuntimeError(f"Apify run missing defaultDatasetId: {json.dumps(run, ensure_ascii=False)}")

        for raw in _apify_iter_dataset_items(cfg=cfg, dataset_id=str(dataset_id)):
            fetched_total += 1
            ts_raw = raw.get("timestamp")
            if not ts_raw:
                continue
            try:
                ts = _parse_iso_dt(ts_raw)
            except Exception:
                continue
            if ts < start_dt or ts >= end_dt:
                continue

            key = _item_key(raw)
            url = (raw.get("url") or "").strip() or None
            short_code = (raw.get("shortCode") or "").strip() or None
            media_type = (raw.get("type") or "").strip() or None
            caption = raw.get("caption") or ""

            likes = _to_int(raw.get("likesCount"))
            comments = _to_int(raw.get("commentsCount"))
            views = _to_int(raw.get("videoViewCount") or raw.get("playsCount") or raw.get("viewCount"))

            content_kind = _content_kind_from_url(url) if url else "unknown"
            handle = _infer_handle(target.profile_url)

            now = _utc_now().isoformat()
            ins = conn.execute(
                """
                INSERT OR IGNORE INTO items(
                    key, profile_url, profile_label, profile_handle, content_kind,
                    url, short_code, media_type, timestamp_utc,
                    likes_count, comments_count, views_count,
                    caption, raw_json, inserted_at_utc, updated_at_utc
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    key,
                    target.profile_url,
                    target.label,
                    handle,
                    content_kind,
                    url,
                    short_code,
                    media_type,
                    ts.isoformat(),
                    likes,
                    comments,
                    views,
                    caption,
                    json.dumps(raw, ensure_ascii=False),
                    now,
                    now,
                ),
            )
            if int(ins.rowcount or 0) == 1:
                inserted_total += 1
            else:
                upd = conn.execute(
                    """
                    UPDATE items
                    SET profile_label = ?,
                        profile_handle = ?,
                        content_kind = ?,
                        url = ?,
                        short_code = ?,
                        media_type = ?,
                        timestamp_utc = ?,
                        likes_count = ?,
                        comments_count = ?,
                        views_count = ?,
                        caption = ?,
                        raw_json = ?,
                        updated_at_utc = ?
                    WHERE key = ?
                    """,
                    (
                        target.label,
                        handle,
                        content_kind,
                        url,
                        short_code,
                        media_type,
                        ts.isoformat(),
                        likes,
                        comments,
                        views,
                        caption,
                        json.dumps(raw, ensure_ascii=False),
                        now,
                        key,
                    ),
                )
                updated_total += int(upd.rowcount or 0)

        conn.commit()

    mark_cached_complete(conn, target.profile_url, target.start, target.end, inserted_total)
    return {
        "profile_url": target.profile_url,
        "label": target.label,
        "skipped": False,
        "fetched": fetched_total,
        "inserted": inserted_total,
        "updated": updated_total,
    }


def export_period_outputs(cfg: Config, conn: sqlite3.Connection, targets: list[Target]) -> dict[str, Path]:
    periods = sorted({(t.start.isoformat(), t.end.isoformat()) for t in targets})
    outputs: dict[str, Path] = {}

    cfg.output_dir.mkdir(parents=True, exist_ok=True)

    for start_s, end_s in periods:
        tag = f"{start_s}_to_{end_s}"
        items_csv = cfg.output_dir / f"items_{tag}.csv"
        summary_csv = cfg.output_dir / f"summary_{tag}.csv"
        llm_json = cfg.output_dir / f"llm_insights_{tag}.json"
        report_html = cfg.output_dir / f"report_{tag}.html"

        period_targets = [t for t in targets if t.start.isoformat() == start_s and t.end.isoformat() == end_s]
        profile_urls = [t.profile_url for t in period_targets]

        rows = conn.execute(
            f"""
            SELECT profile_label, profile_url, content_kind, media_type, timestamp_utc,
                   url, short_code, likes_count, comments_count, views_count, caption
            FROM items
            WHERE profile_url IN ({",".join(["?"] * len(profile_urls))})
              AND timestamp_utc >= ? AND timestamp_utc < ?
            ORDER BY profile_label, timestamp_utc DESC
            """,
            (*profile_urls, f"{start_s}T00:00:00+00:00", f"{end_s}T00:00:00+00:00"),
        ).fetchall()

        with items_csv.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "profile",
                    "profile_url",
                    "kind",
                    "media_type",
                    "timestamp_utc",
                    "url",
                    "shortCode",
                    "likes",
                    "comments",
                    "views",
                    "caption",
                ]
            )
            for r in rows:
                writer.writerow(r)

        summary_rows = []
        for t in period_targets:
            pr = conn.execute(
                """
                SELECT
                  COUNT(*) as items_total,
                  SUM(CASE WHEN content_kind='reel' THEN 1 ELSE 0 END) as reels_count,
                  SUM(CASE WHEN content_kind='post' THEN 1 ELSE 0 END) as posts_count,
                  SUM(COALESCE(likes_count,0)) as likes_sum,
                  SUM(COALESCE(comments_count,0)) as comments_sum,
                  SUM(COALESCE(views_count,0)) as views_sum,
                  SUM(CASE WHEN views_count IS NOT NULL THEN 1 ELSE 0 END) as views_items
                FROM items
                WHERE profile_url = ?
                  AND timestamp_utc >= ? AND timestamp_utc < ?
                """,
                (t.profile_url, f"{start_s}T00:00:00+00:00", f"{end_s}T00:00:00+00:00"),
            ).fetchone()
            if not pr:
                continue

            items_total, reels_count, posts_count, likes_sum, comments_sum, views_sum, views_items = pr
            likes_avg = (likes_sum / items_total) if items_total else 0
            comments_avg = (comments_sum / items_total) if items_total else 0
            views_avg = (views_sum / views_items) if views_items else 0

            top_like = conn.execute(
                """
                SELECT url, likes_count
                FROM items
                WHERE profile_url = ?
                  AND timestamp_utc >= ? AND timestamp_utc < ?
                  AND likes_count IS NOT NULL
                ORDER BY likes_count DESC
                LIMIT 1
                """,
                (t.profile_url, f"{start_s}T00:00:00+00:00", f"{end_s}T00:00:00+00:00"),
            ).fetchone()
            top_view = conn.execute(
                """
                SELECT url, views_count
                FROM items
                WHERE profile_url = ?
                  AND timestamp_utc >= ? AND timestamp_utc < ?
                  AND views_count IS NOT NULL
                ORDER BY views_count DESC
                LIMIT 1
                """,
                (t.profile_url, f"{start_s}T00:00:00+00:00", f"{end_s}T00:00:00+00:00"),
            ).fetchone()

            summary_rows.append(
                [
                    t.label,
                    t.profile_url,
                    items_total,
                    int(posts_count or 0),
                    int(reels_count or 0),
                    int(likes_sum or 0),
                    round(likes_avg, 2),
                    int(comments_sum or 0),
                    round(comments_avg, 2),
                    int(views_sum or 0),
                    round(views_avg, 2),
                    (top_like[0] if top_like else ""),
                    (top_like[1] if top_like else ""),
                    (top_view[0] if top_view else ""),
                    (top_view[1] if top_view else ""),
                ]
            )

        with summary_csv.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "profile",
                    "profile_url",
                    "items_total",
                    "posts_count",
                    "reels_count",
                    "likes_sum",
                    "likes_avg_per_item",
                    "comments_sum",
                    "comments_avg_per_item",
                    "views_sum",
                    "views_avg_per_viewed_item",
                    "top_post_by_likes_url",
                    "top_post_by_likes",
                    "top_by_views_url",
                    "top_by_views",
                ]
            )
            writer.writerows(summary_rows)

        llm_for_report: dict[str, Any] = {}
        llm_json_for_report: Path | None = None
        if cfg.analysis_mode == "llm":
            try:
                llm_for_report = _build_llm_insights(
                    rows=rows,
                    start_s=start_s,
                    end_s=end_s,
                    model=cfg.ollama_model,
                    base_url=cfg.ollama_base_url,
                )
            except Exception as e:
                llm_for_report = {
                    "provider": "ollama",
                    "base_url": cfg.ollama_base_url,
                    "model": cfg.ollama_model,
                    "period": {"start": start_s, "end": end_s},
                    "profiles": [],
                    "error": str(e),
                }
            llm_json.write_text(json.dumps(llm_for_report, ensure_ascii=False, indent=2), encoding="utf-8")
            llm_json_for_report = llm_json

        _write_html_report(
            output_path=report_html,
            start_s=start_s,
            end_s=end_s,
            items_csv=items_csv,
            summary_csv=summary_csv,
            llm_json=llm_json_for_report,
            summary_rows=summary_rows,
            top_likes=_top_items(conn, profile_urls, start_s, end_s, order_by="likes_count", limit=10),
            top_views=_top_items(conn, profile_urls, start_s, end_s, order_by="views_count", limit=10),
            llm_for_report=llm_for_report,
        )

        outputs[f"items_{tag}"] = items_csv
        outputs[f"summary_{tag}"] = summary_csv
        if llm_json_for_report and llm_json_for_report.exists():
            outputs[f"llm_insights_{tag}"] = llm_json_for_report
        outputs[f"report_{tag}"] = report_html

    return outputs


def _publish_outputs(publish_dir: Path, outputs: dict[str, Path]) -> Path:
    publish_dir.mkdir(parents=True, exist_ok=True)

    for _, p in outputs.items():
        if not p.exists():
            continue
        (publish_dir / p.name).write_bytes(p.read_bytes())

    all_files = list(publish_dir.glob("*"))
    reports = sorted([p for p in all_files if p.name.startswith("report_") and p.suffix == ".html"])
    data_files = sorted([p for p in all_files if p.suffix.lower() in {".csv", ".json"}])

    def esc(s: str) -> str:
        return html.escape(s)

    def href(name: str) -> str:
        return esc(name)

    def period_from(name: str) -> str:
        m = re.match(r"^[a-z_]+_(\d{4}-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2})\.(?:html|csv|json)$", name)
        return m.group(1) if m else "unknown"

    files_by_period: dict[str, list[Path]] = defaultdict(list)
    for p in data_files:
        files_by_period[period_from(p.name)].append(p)

    cards_html = []
    for rep in sorted(reports, key=lambda p: p.name, reverse=True):
        per = period_from(rep.name)
        file_list = [p for p in files_by_period.get(per, []) if p.name.startswith(("items_", "summary_", "llm_insights_"))]

        def sort_key(p: Path) -> tuple[int, str]:
            if p.name.startswith("summary_"):
                return (0, p.name)
            if p.name.startswith("items_"):
                return (1, p.name)
            if p.name.startswith("llm_insights_"):
                return (2, p.name)
            return (9, p.name)

        file_list = sorted(file_list, key=sort_key)

        def file_link(p: Path) -> str:
            return f'<a class="chip" href="{href(p.name)}">{esc(p.name.replace(per + "_", ""))}</a>'

        cards_html.append(
            f"""
            <div class="card">
              <div class="card-top">
                <div>
                  <div class="card-title">Период</div>
                  <div class="card-period">{esc(per.replace("_to_", " → "))}</div>
                </div>
                <a class="btn" href="{href(rep.name)}">Открыть отчёт</a>
              </div>
              <div class="muted">Файлы данных:</div>
              <div class="chips">
                {''.join(file_link(p) for p in file_list) if file_list else '<span class="muted">Файлы не найдены</span>'}
              </div>
            </div>
            """
        )

    idx = publish_dir / "index.html"
    idx.write_text(
        f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Instagram reports</title>
  <style>
    :root {{
      --bg: #0b1220;
      --card: #111a2e;
      --text: #e9eefc;
      --muted: #9fb0d0;
      --grid: rgba(255,255,255,0.08);
      --accent: #7aa2ff;
      --btn: #4f7dff;
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      --sans: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, "Noto Sans", "Liberation Sans", sans-serif;
    }}
    body {{
      margin: 0; padding: 24px;
      background: radial-gradient(1200px 800px at 20% -10%, rgba(122,162,255,0.25), transparent 60%),
                  radial-gradient(1200px 800px at 90% 10%, rgba(79,125,255,0.18), transparent 55%),
                  var(--bg);
      color: var(--text);
      font-family: var(--sans);
    }}
    .wrap {{ max-width: 1100px; margin: 0 auto; }}
    a {{ color: var(--accent); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .title {{ font-size: 24px; font-weight: 800; margin: 0; }}
    .sub {{ color: var(--muted); margin-top: 6px; line-height: 1.4; }}
    .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 14px; margin-top: 16px; }}
    .card {{
      background: linear-gradient(180deg, rgba(255,255,255,0.04), transparent 30%), var(--card);
      border: 1px solid var(--grid);
      border-radius: 16px;
      padding: 16px;
    }}
    .card-top {{ display:flex; align-items:flex-start; justify-content:space-between; gap: 10px; }}
    .card-title {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; }}
    .card-period {{ font-family: var(--mono); font-size: 14px; margin-top: 6px; }}
    .muted {{ color: var(--muted); font-size: 13px; margin-top: 12px; }}
    .btn {{
      display:inline-flex; align-items:center; justify-content:center;
      background: linear-gradient(90deg, var(--btn), rgba(122,162,255,0.9));
      color: white; border: 0; border-radius: 999px;
      padding: 10px 12px; font-weight: 700; font-size: 13px;
      text-decoration: none;
      white-space: nowrap;
    }}
    .btn:hover {{ text-decoration: none; filter: brightness(1.05); }}
    .chips {{ display:flex; flex-wrap: wrap; gap: 8px; margin-top: 10px; }}
    .chip {{
      display:inline-flex; align-items:center;
      border: 1px solid var(--grid);
      background: rgba(255,255,255,0.03);
      padding: 6px 10px; border-radius: 999px;
      font-size: 13px;
    }}
    .howto {{
      margin-top: 18px;
      border: 1px dashed var(--grid);
      border-radius: 16px;
      padding: 14px 16px;
      background: rgba(255,255,255,0.02);
    }}
    code {{
      font-family: var(--mono);
      background: rgba(255,255,255,0.06);
      border: 1px solid var(--grid);
      padding: 2px 6px;
      border-radius: 8px;
    }}
    @media (max-width: 980px) {{ .grid {{ grid-template-columns: 1fr; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1 class="title">Instagram reports</h1>
    <div class="sub">
      Статический отчёт (GitHub Pages). Данные собраны через
      <a href="https://apify.com/apify/instagram-scraper" target="_blank" rel="noopener noreferrer">apify/instagram-scraper</a>.
    </div>

    <div class="grid">
      {''.join(cards_html) if cards_html else '<div class="card"><div class="muted">Пока нет отчётов. Сгенерируй docs/ через --publish-dir.</div></div>'}
    </div>

    <div class="howto">
      <div class="card-title">Как обновить</div>
      <div class="sub">Локально (перескрапить и обновить docs):</div>
      <div style="margin-top:10px"><code>./.venv/bin/python ig_apify_scrape.py --publish-dir docs --refresh always --analysis llm</code></div>
      <div class="sub" style="margin-top:10px">Авто (если новых постов нет, перескрапа не будет):</div>
      <div style="margin-top:10px"><code>./.venv/bin/python ig_apify_scrape.py --publish-dir docs --refresh auto --analysis llm</code></div>
      <div class="sub" style="margin-top:10px">Если Ollama недоступен (только скрапинг + отчёт без LLM‑секции):</div>
      <div style="margin-top:10px"><code>./.venv/bin/python ig_apify_scrape.py --publish-dir docs --refresh always --analysis none</code></div>
    </div>
  </div>
</body>
</html>
""",
        encoding="utf-8",
    )
    return idx


def _top_items(conn: sqlite3.Connection, profile_urls: list[str], start_s: str, end_s: str, order_by: str, limit: int) -> list[dict[str, Any]]:
    if not profile_urls:
        return []
    if order_by not in {"likes_count", "views_count"}:
        raise ValueError("order_by must be likes_count or views_count")

    rows = conn.execute(
        f"""
        SELECT profile_label, timestamp_utc, url, content_kind, media_type,
               likes_count, comments_count, views_count, caption
        FROM items
        WHERE profile_url IN ({",".join(["?"] * len(profile_urls))})
          AND timestamp_utc >= ? AND timestamp_utc < ?
          AND {order_by} IS NOT NULL
        ORDER BY {order_by} DESC
        LIMIT ?
        """,
        (*profile_urls, f"{start_s}T00:00:00+00:00", f"{end_s}T00:00:00+00:00", int(limit)),
    ).fetchall()

    out: list[dict[str, Any]] = []
    for (
        profile_label,
        timestamp_utc,
        url,
        content_kind,
        media_type,
        likes_count,
        comments_count,
        views_count,
        caption,
    ) in rows:
        out.append(
            {
                "profile": profile_label,
                "timestamp_utc": timestamp_utc,
                "url": url,
                "kind": content_kind,
                "media_type": media_type,
                "likes": likes_count,
                "comments": comments_count,
                "views": views_count,
                "caption": caption or "",
            }
        )
    return out


def _write_html_report(
    *,
    output_path: Path,
    start_s: str,
    end_s: str,
    items_csv: Path,
    summary_csv: Path,
    llm_json: Path | None,
    summary_rows: list[list[Any]],
    top_likes: list[dict[str, Any]],
    top_views: list[dict[str, Any]],
    llm_for_report: dict[str, Any],
) -> None:
    def esc(s: Any) -> str:
        return html.escape("" if s is None else str(s))

    def link(url: Any) -> str:
        u = ("" if url is None else str(url)).strip()
        if not u:
            return ""
        u_esc = esc(u)
        return f'<a href="{u_esc}" target="_blank" rel="noopener noreferrer">{u_esc}</a>'

    def trunc(s: str, n: int = 180) -> str:
        s = s.strip()
        if len(s) <= n:
            return s
        return s[: n - 1] + "…"

    profiles = [r[0] for r in summary_rows]
    likes_sum = [int(r[5] or 0) for r in summary_rows]
    views_sum = [int(r[9] or 0) for r in summary_rows]
    items_total = [int(r[2] or 0) for r in summary_rows]

    def bar_block(title: str, labels: list[str], values: list[int]) -> str:
        mx = max(values) if values else 0
        rows_html = []
        for lab, val in sorted(zip(labels, values), key=lambda t: t[1], reverse=True):
            pct = (val / mx * 100.0) if mx else 0.0
            rows_html.append(
                f"""
                <div class="bar-row">
                  <div class="bar-label">{esc(lab)}</div>
                  <div class="bar-track"><div class="bar-fill" style="width:{pct:.2f}%"></div></div>
                  <div class="bar-value">{esc(val)}</div>
                </div>
                """
            )
        return f"""
        <div class="card">
          <div class="card-title">{esc(title)}</div>
          <div class="bar-list">
            {''.join(rows_html)}
          </div>
        </div>
        """

    def top_table(title: str, items: list[dict[str, Any]]) -> str:
        trs = []
        for it in items:
            trs.append(
                "<tr>"
                f"<td>{esc(it.get('profile'))}</td>"
                f"<td class='mono'>{esc(it.get('timestamp_utc'))}</td>"
                f"<td>{link(it.get('url'))}</td>"
                f"<td>{esc(it.get('kind'))}</td>"
                f"<td>{esc(it.get('media_type'))}</td>"
                f"<td class='num'>{esc(it.get('likes'))}</td>"
                f"<td class='num'>{esc(it.get('comments'))}</td>"
                f"<td class='num'>{esc(it.get('views'))}</td>"
                f"<td class='caption'>{esc(trunc(it.get('caption') or ''))}</td>"
                "</tr>"
            )
        return f"""
        <div class="card">
          <div class="card-title">{esc(title)}</div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>profile</th>
                  <th>timestamp_utc</th>
                  <th>url</th>
                  <th>kind</th>
                  <th>media_type</th>
                  <th class="num">likes</th>
                  <th class="num">comments</th>
                  <th class="num">views</th>
                  <th>caption</th>
                </tr>
              </thead>
              <tbody>
                {''.join(trs) if trs else '<tr><td colspan="9" class="muted">Нет данных</td></tr>'}
              </tbody>
            </table>
          </div>
        </div>
        """

    summary_trs = []
    for r in summary_rows:
        summary_trs.append(
            "<tr>"
            f"<td>{esc(r[0])}</td>"
            f"<td>{link(r[1])}</td>"
            f"<td class='num'>{esc(r[2])}</td>"
            f"<td class='num'>{esc(r[3])}</td>"
            f"<td class='num'>{esc(r[4])}</td>"
            f"<td class='num'>{esc(r[5])}</td>"
            f"<td class='num'>{esc(r[6])}</td>"
            f"<td class='num'>{esc(r[7])}</td>"
            f"<td class='num'>{esc(r[8])}</td>"
            f"<td class='num'>{esc(r[9])}</td>"
            f"<td class='num'>{esc(r[10])}</td>"
            f"<td>{link(r[11])}</td>"
            f"<td class='num'>{esc(r[12])}</td>"
            f"<td>{link(r[13])}</td>"
            f"<td class='num'>{esc(r[14])}</td>"
            "</tr>"
        )

    rel_items = esc(items_csv.name)
    rel_summary = esc(summary_csv.name)
    rel_llm_json = esc(llm_json.name) if llm_json else ""
    llm_link = f'<a class="pill" href="{rel_llm_json}">Скачать llm_insights JSON</a>' if llm_json else ""

    doc = f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Instagram report {esc(start_s)} → {esc(end_s)}</title>
  <style>
    :root {{
      --bg: #0b1220;
      --card: #111a2e;
      --text: #e9eefc;
      --muted: #9fb0d0;
      --grid: rgba(255,255,255,0.08);
      --accent: #7aa2ff;
      --bar: #4f7dff;
      --mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      --sans: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, "Noto Sans", "Liberation Sans", sans-serif;
    }}
    body {{
      margin: 0; padding: 24px;
      background: radial-gradient(1200px 800px at 20% -10%, rgba(122,162,255,0.25), transparent 60%),
                  radial-gradient(1200px 800px at 90% 10%, rgba(79,125,255,0.18), transparent 55%),
                  var(--bg);
      color: var(--text);
      font-family: var(--sans);
    }}
    .wrap {{ max-width: 1200px; margin: 0 auto; }}
    .header {{
      display: flex; align-items: flex-end; justify-content: space-between; gap: 16px;
      margin-bottom: 16px;
    }}
    .title {{ font-size: 22px; font-weight: 700; }}
    .subtitle {{ color: var(--muted); font-size: 13px; margin-top: 6px; }}
    a {{ color: var(--accent); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .grid {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px; margin: 14px 0; }}
    .card {{
      background: linear-gradient(180deg, rgba(255,255,255,0.04), transparent 30%), var(--card);
      border: 1px solid var(--grid);
      border-radius: 14px;
      padding: 14px;
      overflow: hidden;
    }}
    .card-title {{ font-weight: 700; margin-bottom: 10px; }}
    .bar-row {{ display: grid; grid-template-columns: 140px 1fr 90px; gap: 10px; align-items: center; margin: 8px 0; }}
    .bar-label {{ color: var(--muted); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
    .bar-track {{ background: rgba(255,255,255,0.06); border-radius: 999px; height: 10px; overflow: hidden; }}
    .bar-fill {{ height: 100%; background: linear-gradient(90deg, var(--bar), rgba(122,162,255,0.8)); }}
    .bar-value {{ text-align: right; font-family: var(--mono); font-size: 12px; }}
    .table-wrap {{ overflow-x: auto; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ border-bottom: 1px solid var(--grid); padding: 9px 10px; vertical-align: top; }}
    th {{ text-align: left; color: var(--muted); font-weight: 600; }}
    tbody tr:hover {{ background: rgba(255,255,255,0.03); }}
    td.num, th.num {{ text-align: right; font-family: var(--mono); }}
    .mono {{ font-family: var(--mono); font-size: 12px; white-space: nowrap; }}
    .caption {{ max-width: 420px; color: rgba(233,238,252,0.92); }}
    .muted {{ color: var(--muted); }}
    .links {{ display:flex; gap: 12px; flex-wrap: wrap; }}
    .pill {{
      display:inline-flex; align-items:center; gap:8px;
      padding: 8px 10px; border-radius: 999px;
      border: 1px solid var(--grid); background: rgba(255,255,255,0.03);
      font-size: 13px;
    }}
    .footer {{ margin-top: 18px; color: var(--muted); font-size: 12px; }}
    details.card > summary {{ cursor: pointer; list-style: none; }}
    details.card > summary::-webkit-details-marker {{ display: none; }}
    .chips {{ display:flex; flex-wrap:wrap; gap: 8px; margin-top: 4px; }}
    .chip {{ display:inline-flex; align-items:center; padding: 4px 10px; border-radius: 999px; border: 1px solid var(--grid); background: rgba(255,255,255,0.03); font-size: 12px; }}
    @media (max-width: 980px) {{ .grid {{ grid-template-columns: 1fr; }} .bar-row {{ grid-template-columns: 120px 1fr 80px; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="header">
      <div>
        <div class="title">Instagram report</div>
        <div class="subtitle">Период: <span class="mono">{esc(start_s)} → {esc(end_s)}</span> (end-exclusive, UTC)</div>
        <div class="subtitle">Что внутри: KPI по аккаунтам → топ‑посты → LLM‑анализ (только LLM).</div>
      </div>
      <div class="links">
        <a class="pill" href="{rel_items}">Скачать items CSV</a>
        <a class="pill" href="{rel_summary}">Скачать summary CSV</a>
        {llm_link}
      </div>
    </div>

    <div class="grid">
      {bar_block("Постов/рилсов (шт)", profiles, items_total)}
      {bar_block("Лайки (сумма)", profiles, likes_sum)}
      {bar_block("Просмотры/plays (сумма, где доступно)", profiles, views_sum)}
    </div>

    <div class="card">
      <div class="card-title">Сводка по аккаунтам</div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>profile</th>
              <th>profile_url</th>
              <th class="num">items_total</th>
              <th class="num">posts</th>
              <th class="num">reels</th>
              <th class="num">likes_sum</th>
              <th class="num">likes_avg</th>
              <th class="num">comments_sum</th>
              <th class="num">comments_avg</th>
              <th class="num">views_sum</th>
              <th class="num">views_avg*</th>
              <th>top_like_url</th>
              <th class="num">top_like</th>
              <th>top_view_url</th>
              <th class="num">top_view</th>
            </tr>
          </thead>
          <tbody>
            {''.join(summary_trs) if summary_trs else '<tr><td colspan="15" class="muted">Нет данных</td></tr>'}
          </tbody>
        </table>
      </div>
      <div class="footer">* views_avg считается только по постам/рилсам, где Apify вернул views/plays.</div>
    </div>

    {top_table("Топ‑10 по лайкам (все аккаунты)", top_likes)}
    {top_table("Топ‑10 по просмотрам/plays (все аккаунты)", top_views)}

    {_llm_section_html(llm_for_report)}

    <div class="footer">
      Источник данных: Apify actor <a href="https://apify.com/apify/instagram-scraper" target="_blank" rel="noopener noreferrer">apify/instagram-scraper</a>.
    </div>
  </div>
</body>
</html>
"""

    output_path.write_text(doc, encoding="utf-8")


def _llm_section_html(llm: dict[str, Any]) -> str:
    if not llm:
        return (
            "<div class='card'>"
            "<div class='card-title'>LLM анализ</div>"
            "<div class='muted'>LLM‑анализ не сгенерирован. Запусти скрипт с <code>--analysis llm</code> и локальным Ollama.</div>"
            "</div>"
        )

    def esc(s: Any) -> str:
        return html.escape("" if s is None else str(s))

    model = llm.get("model") or ""
    error = llm.get("error")
    if error:
        return (
            "<div class='card'>"
            "<div class='card-title'>LLM анализ (только LLM)</div>"
            f"<div class='muted'>Не удалось получить LLM‑анализ через Ollama. Ошибка: <code>{esc(error)}</code>.</div>"
            f"<div class='muted' style='margin-top:8px'>Модель: <code>{esc(model)}</code>.</div>"
            "</div>"
        )

    blocks = []
    for p in llm.get("profiles", []) or []:
        prof = p.get("profile") or ""
        recs = p.get("recommendations") or []
        themes = p.get("themes") or []

        def li(items):
            if not items:
                return "<div class='muted'>Нет</div>"
            return "<ul>" + "".join(f"<li>{esc(x)}</li>" for x in items) + "</ul>"

        blocks.append(
            f"""
            <details class="card" open>
              <summary class="card-title">LLM: {esc(prof)}</summary>
              <div class="grid" style="margin-top:12px">
                <div class="card"><div class="card-title">Темы</div>{li(themes)}</div>
                <div class="card"><div class="card-title">Рекомендации</div>{li(recs)}</div>
              </div>
            </details>
            """
        )

    return (
        "<div class='card'>"
        "<div class='card-title'>LLM анализ (только LLM)</div>"
        f"<div class='muted'>Модель: <code>{esc(model)}</code>. "
        "Это генеративный анализ текстов постов + метрик. Результат сохраняется в <code>llm_insights_*.json</code>.</div>"
        "</div>"
        + "".join(blocks)
    )


def _ollama_chat_json(*, base_url: str, model: str, messages: list[dict[str, str]], timeout: int = 120) -> dict[str, Any]:
    url = base_url.rstrip("/") + "/api/chat"
    body = {
        "model": model,
        "messages": messages,
        "stream": False,
        "options": {"temperature": 0.2},
    }
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url=url, data=data, method="POST", headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except Exception as e:
        raise RuntimeError(f"Ollama request failed: {e}") from e


def _build_llm_insights(*, rows: list[tuple[Any, ...]], start_s: str, end_s: str, model: str, base_url: str) -> dict[str, Any]:
    by_profile: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    for r in rows:
        by_profile[str(r[0] or "")].append(r)

    out_profiles = []
    for profile, items in sorted(by_profile.items()):
        def to_int(x):
            try:
                return int(x)
            except Exception:
                return None

        items_sorted_likes = sorted(items, key=lambda r: to_int(r[7]) or 0, reverse=True)
        items_sorted_views = sorted(items, key=lambda r: to_int(r[9]) or 0, reverse=True)
        items_sorted_time = sorted(items, key=lambda r: str(r[4] or ""), reverse=True)

        sample = []
        seen = set()
        for src in (items_sorted_likes[:8], items_sorted_views[:8], items_sorted_time[:8]):
            for r in src:
                u = str(r[5] or "")
                if u in seen:
                    continue
                seen.add(u)
                caption = str(r[10] or "")
                caption = caption.replace("\n", " ").strip()
                if len(caption) > 400:
                    caption = caption[:399] + "…"
                sample.append(
                    {
                        "url": u,
                        "timestamp_utc": str(r[4] or ""),
                        "likes": to_int(r[7]),
                        "views": to_int(r[9]),
                        "caption": caption,
                    }
                )
        sample = sample[:20]

        system = (
            "Ты маркетолог-аналитик Instagram. "
            "Твоя задача: по выборке постов и метрикам предложить темы контента и рекомендации. "
            "Отвечай строго JSON объектом формата: "
            '{"themes":[...], "recommendations":[...], "notes":[...]}'
        )
        user = {
            "profile": profile,
            "period": f"{start_s}..{end_s} (UTC, end-exclusive)",
            "posts_sample": sample,
            "requirements": {
                "themes": "список 5-10 тем (кратко, по-русски)",
                "recommendations": "список 5-10 рекомендаций (кратко, по-русски)",
                "notes": "1-3 оговорки про качество данных (например views есть не у всех)",
            },
        }

        resp = _ollama_chat_json(
            base_url=base_url,
            model=model,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": json.dumps(user, ensure_ascii=False)}],
            timeout=180,
        )
        content = ((resp.get("message") or {}).get("content") or "").strip()
        try:
            parsed = json.loads(content)
        except Exception:
            parsed = {"themes": [], "recommendations": [], "notes": [content]}

        out_profiles.append(
            {
                "profile": profile,
                "themes": parsed.get("themes") if isinstance(parsed, dict) else [],
                "recommendations": parsed.get("recommendations") if isinstance(parsed, dict) else [],
                "notes": parsed.get("notes") if isinstance(parsed, dict) else [],
            }
        )

    return {
        "provider": "ollama",
        "base_url": base_url,
        "model": model,
        "period": {"start": start_s, "end": end_s},
        "profiles": out_profiles,
    }


def load_config_from_env(args: argparse.Namespace) -> Config:
    load_dotenv()
    apify_token = (os.environ.get("APIFY_TOKEN") or "").strip()
    if not apify_token:
        raise SystemExit("Не задан APIFY_TOKEN. Скопируй `.env.example` -> `.env` и вставь токен.")

    apify_base_url = (os.environ.get("APIFY_BASE_URL") or "https://api.apify.com").strip()
    output_dir = Path(os.environ.get("OUTPUT_DIR", "output")).expanduser()
    db_path = Path(os.environ.get("DB_PATH", "cache/instagram_apify.sqlite")).expanduser()
    ollama_base_url = (os.environ.get("OLLAMA_BASE_URL") or "http://localhost:11434").strip()
    ollama_model = (os.environ.get("OLLAMA_MODEL") or "llama3.2:3b").strip()

    refresh_mode = (getattr(args, "refresh", None) or "auto").strip().lower()
    if getattr(args, "update", False):
        refresh_mode = "always"
    if refresh_mode not in {"auto", "always", "never"}:
        raise SystemExit("Некорректный --refresh. Используй: auto | always | never")

    analysis_mode = (getattr(args, "analysis", None) or "llm").strip().lower()
    if analysis_mode not in {"llm", "none"}:
        raise SystemExit("Некорректный --analysis. Используй: llm | none")

    return Config(
        apify_token=apify_token,
        apify_base_url=apify_base_url,
        output_dir=output_dir,
        db_path=db_path,
        results_limit_per_type=max(1, int(args.limit)),
        refresh_mode=refresh_mode,
        check_limit=max(1, int(getattr(args, "check_limit", 1) or 1)),
        analysis_mode=analysis_mode,
        ollama_base_url=ollama_base_url,
        ollama_model=ollama_model,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Apify Instagram batch scraper: targets + period + cache + summary + LLM-only analysis")
    parser.add_argument("--targets", default=os.environ.get("TARGETS_FILE", "targets.txt"))
    parser.add_argument("--start", default=None, help="YYYY-MM-DD (UTC)")
    parser.add_argument("--end", default=None, help="YYYY-MM-DD end-exclusive (UTC)")
    parser.add_argument("--period", default=None, help="this-month | last-month | last-30d (если --start/--end не заданы)")
    parser.add_argument("--limit", type=int, default=2000, help="resultsLimit per type (posts/reels) per profile")
    parser.add_argument("--refresh", default="auto", help="auto | always | never (по умолчанию auto)")
    parser.add_argument("--check-limit", dest="check_limit", type=int, default=1, help="Сколько последних постов брать для проверки новых (по умолчанию 1)")
    parser.add_argument("--analysis", default="llm", help="llm | none (по умолчанию llm)")
    parser.add_argument(
        "--publish-dir",
        default=None,
        help="Папка для публикации отчётов (например, docs для GitHub Pages). Скопирует report_*.html + CSV/JSON и создаст docs/index.html",
    )
    parser.add_argument("--update", action="store_true", help="(устарело) то же, что --refresh always")
    args = parser.parse_args()

    default_start, default_end = _resolve_period(args)

    cfg = load_config_from_env(args)
    targets = load_targets(Path(args.targets), default_start=default_start, default_end=default_end)
    if not targets:
        raise SystemExit("targets файл пустой — добавь профили.")

    conn = _connect_db(cfg.db_path)
    try:
        _init_db(conn)
        results = [scrape_one_target(cfg, conn, t) for t in targets]
        outputs = export_period_outputs(cfg, conn, targets)
        published_index: str | None = None
        if args.publish_dir:
            idx = _publish_outputs(Path(args.publish_dir), outputs)
            published_index = str(idx)
    finally:
        conn.close()

    payload: dict[str, Any] = {"results": results, "outputs": {k: str(v) for k, v in outputs.items()}}
    if args.publish_dir:
        payload["published_dir"] = str(args.publish_dir)
        payload["published_index"] = published_index
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

