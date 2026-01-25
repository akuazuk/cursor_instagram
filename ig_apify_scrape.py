from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import os
import re
import sqlite3
import statistics
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
    llm_chunk_size: int
    llm_max_posts_per_profile: int  # 0 = all
    ollama_timeout_seconds: int
    llm_polish: bool
    baseline_profile: str
    vectara_base_url: str
    vectara_customer_id: str
    vectara_api_key: str
    vectara_corpus_key: str
    vectara_enabled: bool
    vectara_index: bool
    vectara_theme_limit: int
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS vectara_index_cache (
            item_key TEXT PRIMARY KEY,
            vectara_doc_id TEXT NOT NULL,
            profile_label TEXT,
            profile_url TEXT,
            indexed_at_utc TEXT NOT NULL
        )
        """
    )
    _ensure_columns(conn)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_items_profile_ts ON items(profile_url, timestamp_utc)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_vectara_doc_id ON vectara_index_cache(vectara_doc_id)")
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


def _percentile(values: list[int], p: float) -> Optional[float]:
    """
    p in [0..100]. Simple nearest-rank percentile.
    """
    if not values:
        return None
    if p <= 0:
        return float(min(values))
    if p >= 100:
        return float(max(values))
    s = sorted(values)
    k = int((p / 100.0) * (len(s) - 1))
    return float(s[max(0, min(len(s) - 1, k))])


def _median(values: list[int]) -> Optional[float]:
    if not values:
        return None
    try:
        return float(statistics.median(values))
    except Exception:
        return None


def _chunked(items: list[Any], chunk_size: int) -> list[list[Any]]:
    if chunk_size <= 0:
        return [items]
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def _parse_llm_json(content: str) -> dict[str, Any]:
    """
    Ollama иногда возвращает JSON с лишним текстом. Пытаемся вытащить объект.
    """
    c = (content or "").strip()
    if not c:
        return {}
    try:
        v = json.loads(c)
        return v if isinstance(v, dict) else {"value": v}
    except Exception:
        pass

    # try extract the first {...} block
    start = c.find("{")
    end = c.rfind("}")
    if start >= 0 and end > start:
        maybe = c[start : end + 1]
        try:
            v = json.loads(maybe)
            return v if isinstance(v, dict) else {"value": v}
        except Exception:
            return {"raw": c}
    return {"raw": c}


def _as_text(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return x.strip()
    if isinstance(x, (int, float, bool)):
        return str(x)
    if isinstance(x, dict):
        # common keys
        for k in ("text", "title", "name", "description", "evidence", "strength", "gap", "pattern", "initiative", "pillar"):
            v = x.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return json.dumps(x, ensure_ascii=False)
    return str(x).strip()


def _join_list(items: Any, limit: int = 6) -> str:
    if not items or not isinstance(items, list):
        return ""
    out: list[str] = []
    for x in items[:limit]:
        t = _as_text(x)
        if t:
            out.append(t)
    return "; ".join(out)


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
        llm_benchmark_csv = cfg.output_dir / f"llm_benchmark_{tag}.csv"
        vs_json = cfg.output_dir / f"llm_vs_{cfg.baseline_profile}_{tag}.json"
        vs_csv = cfg.output_dir / f"benchmark_vs_{cfg.baseline_profile}_{tag}.csv"
        vectara_themes_csv = cfg.output_dir / f"vectara_themes_{tag}.csv"
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

        # Optional: index posts into Vectara for semantic queries.
        vectara_index_info: dict[str, Any] = {}
        if _vectara_is_enabled(cfg):
            try:
                vectara_index_info = _vectara_sync_index(conn=conn, cfg=cfg, rows=rows, timeout=30, sleep_seconds=0.05)
            except Exception as e:
                vectara_index_info = {"enabled": True, "error": str(e), "indexed": 0, "skipped": 0}

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
                  SUM(CASE WHEN likes_count IS NOT NULL AND likes_count >= 0 THEN likes_count ELSE 0 END) as likes_sum,
                  SUM(CASE WHEN comments_count IS NOT NULL AND comments_count >= 0 THEN comments_count ELSE 0 END) as comments_sum,
                  SUM(CASE WHEN views_count IS NOT NULL AND views_count >= 0 THEN views_count ELSE 0 END) as views_sum,
                  SUM(CASE WHEN views_count IS NOT NULL AND views_count >= 0 THEN 1 ELSE 0 END) as views_items
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
        llm_benchmark_for_report: Path | None = None
        vs_json_for_report: Path | None = None
        vs_csv_for_report: Path | None = None
        vs_for_report: dict[str, Any] = {}
        vectara_themes_for_report: Path | None = None
        vectara_themes_for_html: dict[str, Any] = {}
        if cfg.analysis_mode == "llm":
            try:
                llm_for_report, bench_rows = _build_llm_insights(
                    rows=rows,  # all posts in period
                    start_s=start_s,
                    end_s=end_s,
                    model=cfg.ollama_model,
                    base_url=cfg.ollama_base_url,
                    chunk_size=cfg.llm_chunk_size,
                    max_posts_per_profile=cfg.llm_max_posts_per_profile,
                    timeout_seconds=cfg.ollama_timeout_seconds,
                    polish=cfg.llm_polish,
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
                bench_rows = []
            llm_json.write_text(json.dumps(llm_for_report, ensure_ascii=False, indent=2), encoding="utf-8")
            llm_json_for_report = llm_json

            _write_llm_benchmark_csv(llm_benchmark_csv, bench_rows)
            llm_benchmark_for_report = llm_benchmark_csv

            # Baseline comparison (vs Kravira by default)
            try:
                vs_payload, vs_rows = _build_vs_baseline(
                    base_url=cfg.ollama_base_url,
                    model=cfg.ollama_model,
                    timeout_seconds=cfg.ollama_timeout_seconds,
                    baseline=cfg.baseline_profile,
                    bench_rows=bench_rows,
                    all_rows=rows,
                    start_s=start_s,
                    end_s=end_s,
                )
                vs_json.write_text(json.dumps(vs_payload, ensure_ascii=False, indent=2), encoding="utf-8")
                _write_vs_csv(vs_csv, vs_rows)
                vs_json_for_report = vs_json
                vs_csv_for_report = vs_csv
                vs_for_report = vs_payload
            except Exception as e:
                # Don't fail the run if baseline comparison fails.
                vs_payload = {"baseline": cfg.baseline_profile, "error": str(e)}
                vs_json.write_text(json.dumps(vs_payload, ensure_ascii=False, indent=2), encoding="utf-8")
                vs_json_for_report = vs_json
                vs_for_report = vs_payload

        # Vectara theme benchmark (works even if LLM disabled)
        if _vectara_is_enabled(cfg):
            try:
                baseline_label = _pick_baseline_from_rows(rows, cfg.baseline_profile)
                vectara_themes_for_html = _vectara_theme_benchmark(
                    cfg=cfg,
                    baseline_profile=baseline_label,
                    limit=int(cfg.vectara_theme_limit),
                )
                _write_vectara_themes_csv(vectara_themes_csv, vectara_themes_for_html)
                vectara_themes_for_report = vectara_themes_csv
            except Exception as e:
                vectara_themes_for_html = {"error": str(e), "baseline": cfg.baseline_profile}
        elif cfg.vectara_enabled:
            vectara_themes_for_html = {
                "error": "Vectara включена, но не заданы env: VECTARA_CUSTOMER_ID, VECTARA_CORPUS_KEY, VECTARA_API_KEY",
                "baseline": cfg.baseline_profile,
            }

        _write_html_report(
            output_path=report_html,
            start_s=start_s,
            end_s=end_s,
            items_csv=items_csv,
            summary_csv=summary_csv,
            llm_json=llm_json_for_report,
            llm_benchmark_csv=llm_benchmark_for_report,
            vs_json=vs_json_for_report,
            vs_csv=vs_csv_for_report,
            vs_for_report=vs_for_report,
            vectara_themes_csv=vectara_themes_for_report,
            vectara_themes=vectara_themes_for_html,
            vectara_index_info=vectara_index_info,
            summary_rows=summary_rows,
            top_likes=_top_items(conn, profile_urls, start_s, end_s, order_by="likes_count", limit=10),
            top_views=_top_items(conn, profile_urls, start_s, end_s, order_by="views_count", limit=10),
            llm_for_report=llm_for_report,
        )

        outputs[f"items_{tag}"] = items_csv
        outputs[f"summary_{tag}"] = summary_csv
        if llm_json_for_report and llm_json_for_report.exists():
            outputs[f"llm_insights_{tag}"] = llm_json_for_report
        if llm_benchmark_for_report and llm_benchmark_for_report.exists():
            outputs[f"llm_benchmark_{tag}"] = llm_benchmark_for_report
        if vs_json_for_report and vs_json_for_report.exists():
            outputs[f"llm_vs_{tag}"] = vs_json_for_report
        if vs_csv_for_report and vs_csv_for_report.exists():
            outputs[f"benchmark_vs_{tag}"] = vs_csv_for_report
        if vectara_themes_for_report and vectara_themes_for_report.exists():
            outputs[f"vectara_themes_{tag}"] = vectara_themes_for_report
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
        m = re.search(r"(\d{4}-\d{2}-\d{2}_to_\d{4}-\d{2}-\d{2})", name)
        return m.group(1) if m else "unknown"

    files_by_period: dict[str, list[Path]] = defaultdict(list)
    for p in data_files:
        files_by_period[period_from(p.name)].append(p)

    cards_html = []
    for rep in sorted(reports, key=lambda p: p.name, reverse=True):
        per = period_from(rep.name)
        file_list = [
            p
            for p in files_by_period.get(per, [])
            if p.name.startswith(
                (
                    "items_",
                    "summary_",
                    "llm_insights_",
                    "llm_benchmark_",
                    "llm_vs_",
                    "benchmark_vs_",
                    "vectara_themes_",
                )
            )
        ]

        def sort_key(p: Path) -> tuple[int, str]:
            if p.name.startswith("summary_"):
                return (0, p.name)
            if p.name.startswith("items_"):
                return (1, p.name)
            if p.name.startswith("llm_insights_"):
                return (2, p.name)
            if p.name.startswith("llm_benchmark_"):
                return (3, p.name)
            if p.name.startswith("benchmark_vs_"):
                return (4, p.name)
            if p.name.startswith("llm_vs_"):
                return (5, p.name)
            if p.name.startswith("vectara_themes_"):
                return (6, p.name)
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
    llm_benchmark_csv: Path | None,
    summary_rows: list[list[Any]],
    top_likes: list[dict[str, Any]],
    top_views: list[dict[str, Any]],
    llm_for_report: dict[str, Any],
    vs_json: Path | None,
    vs_csv: Path | None,
    vs_for_report: dict[str, Any],
    vectara_themes_csv: Path | None,
    vectara_themes: dict[str, Any],
    vectara_index_info: dict[str, Any],
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
    rel_bench = esc(llm_benchmark_csv.name) if llm_benchmark_csv else ""
    bench_link = f'<a class="pill" href="{rel_bench}">Скачать benchmark CSV</a>' if llm_benchmark_csv else ""
    rel_vs_json = esc(vs_json.name) if vs_json else ""
    rel_vs_csv = esc(vs_csv.name) if vs_csv else ""
    vs_json_link = f'<a class="pill" href="{rel_vs_json}">Скачать сравнение vs {esc((vs_for_report or {}).get("baseline") or "baseline")} (JSON)</a>' if vs_json else ""
    vs_csv_link = f'<a class="pill" href="{rel_vs_csv}">Скачать сравнение vs {esc((vs_for_report or {}).get("baseline") or "baseline")} (CSV)</a>' if vs_csv else ""
    rel_vectara = esc(vectara_themes_csv.name) if vectara_themes_csv else ""
    vectara_link = f'<a class="pill" href="{rel_vectara}">Скачать Vectara темы (CSV)</a>' if vectara_themes_csv else ""

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
        {bench_link}
        {vs_csv_link}
        {vs_json_link}
        {vectara_link}
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
    {_vs_baseline_section_html(vs_for_report)}
    {_vectara_themes_section_html(vectara_themes, vectara_index_info)}

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

    benchmark = llm.get("benchmark") or {}
    bench_rows = benchmark.get("rows") or []
    bench_cols = benchmark.get("columns") or []

    def bench_table() -> str:
        if not bench_cols or not bench_rows:
            return "<div class='muted'>Нет сравнительной таблицы</div>"
        ths = "".join(f"<th>{esc(c)}</th>" for c in bench_cols)
        trs = []
        for r in bench_rows:
            trs.append("<tr>" + "".join(f"<td>{esc(r.get(c,''))}</td>" for c in bench_cols) + "</tr>")
        return "<div class='table-wrap'><table><thead><tr>" + ths + "</tr></thead><tbody>" + "".join(trs) + "</tbody></table></div>"

    def bullets(items: Any) -> str:
        if not items:
            return "<div class='muted'>Нет</div>"
        if isinstance(items, str):
            return f"<div>{esc(items)}</div>"
        if isinstance(items, list):
            return "<ul>" + "".join(f"<li>{esc(_as_text(x))}</li>" for x in items) + "</ul>"
        return f"<div>{esc(items)}</div>"

    def pills(arr: Any) -> str:
        if not arr or not isinstance(arr, list):
            return ""
        return "<div class='chips'>" + "".join(f"<span class='chip'>{esc(x)}</span>" for x in arr[:12]) + "</div>"

    blocks = []
    for p in llm.get("profiles", []) or []:
        prof = p.get("profile") or ""
        positioning = p.get("positioning") or ""
        audience = p.get("audience") or []
        drivers = p.get("performance_drivers") or []
        strengths = p.get("strengths") or []
        gaps = p.get("gaps") or []
        plan = p.get("action_plan_30d") or []
        pillars = p.get("content_pillars") or []
        kpis = p.get("kpis") or {}

        def kpi_line() -> str:
            if not kpis:
                return ""
            return (
                "<div class='muted'>"
                f"Постов: <span class='mono'>{esc(kpis.get('items_total'))}</span>, "
                f"reels: <span class='mono'>{esc(kpis.get('reels_count'))}</span>, "
                f"likes avg: <span class='mono'>{esc(kpis.get('likes_avg'))}</span>, "
                f"views avg*: <span class='mono'>{esc(kpis.get('views_avg'))}</span>"
                "</div>"
            )

        def pillars_block() -> str:
            if not pillars:
                return "<div class='muted'>Нет</div>"
            lis = []
            for it in pillars[:10]:
                if isinstance(it, dict):
                    title = it.get("pillar") or ""
                    share = it.get("share_pct")
                    fmt = it.get("best_formats") or []
                else:
                    title = _as_text(it)
                    share = None
                    fmt = []
                lis.append(
                    "<li>"
                    f"<b>{esc(title)}</b>"
                    + (f" — {esc(share)}%" if share is not None else "")
                    + (pills(fmt) if fmt else "")
                    + "</li>"
                )
            return "<ul>" + "".join(lis) + "</ul>"

        blocks.append(
            f"""
            <details class="card" open>
              <summary class="card-title">LLM: {esc(prof)}</summary>
              {kpi_line()}
              <div class="grid" style="margin-top:12px">
                <div class="card">
                  <div class="card-title">Позиционирование</div>
                  <div>{esc(positioning)}</div>
                  <div class="card-title" style="margin-top:12px">Аудитория</div>
                  {bullets(audience)}
                </div>
                <div class="card">
                  <div class="card-title">Контент‑пиллары</div>
                  {pillars_block()}
                </div>
                <div class="card">
                  <div class="card-title">Драйверы метрик</div>
                  {bullets(drivers)}
                </div>
                <div class="card">
                  <div class="card-title">Сильные стороны</div>
                  {bullets(strengths)}
                </div>
                <div class="card">
                  <div class="card-title">Пробелы/риски</div>
                  {bullets(gaps)}
                </div>
                <div class="card">
                  <div class="card-title">План на 30 дней (тесты)</div>
                  {bullets([f"{x.get('priority','')} {x.get('initiative','')}: {x.get('hypothesis','')} | тест: {x.get('test','')} | метрика: {x.get('success_metric','')} | эффект: {x.get('expected_impact','')}" for x in plan] if isinstance(plan, list) else plan)}
                </div>
              </div>
            </details>
            """
        )

    return (
        "<div class='card'>"
        "<div class='card-title'>LLM анализ (все посты, профессиональный бенчмарк)</div>"
        f"<div class='muted'>Модель: <code>{esc(model)}</code>. "
        "LLM получает полный список постов за период (чанками) + KPI и собирает сравнительный бенчмарк центров.</div>"
        "</div>"
        + "<div class='card'><div class='card-title'>Сравнительная таблица центров</div>"
        + bench_table()
        + "</div>"
        + "".join(blocks)
    )


def _vs_baseline_section_html(vs: dict[str, Any]) -> str:
    def esc(s: Any) -> str:
        return html.escape("" if s is None else str(s))

    if not vs:
        return (
            "<div class='card'>"
            "<div class='card-title'>Сравнение с baseline (например, Кравира)</div>"
            "<div class='muted'>Нет данных сравнения.</div>"
            "</div>"
        )

    err = vs.get("error")
    baseline = vs.get("baseline") or vs.get("baseline_profile") or "baseline"
    if err:
        return (
            "<div class='card'>"
            f"<div class='card-title'>Сравнение с {esc(baseline)}</div>"
            f"<div class='muted'>Не удалось построить сравнение: <code>{esc(err)}</code></div>"
            "</div>"
        )

    rows = vs.get("vs_table") or []
    if not isinstance(rows, list):
        rows = []

    # Table
    cols = [
        ("competitor", "конкурент"),
        ("posts_per_week_ratio", "частота×"),
        ("likes_median_ratio", "лайки p50×"),
        ("likes_p75_ratio", "лайки p75×"),
        ("comments_avg_ratio", "комменты×"),
        ("views_median_ratio", "просмотры p50×"),
        ("views_p75_ratio", "просмотры p75×"),
        ("top_advantages", "топ‑преимущества"),
    ]
    ths = "".join(f"<th>{esc(title)}</th>" for _, title in cols)
    trs = []
    for r in rows:
        trs.append("<tr>" + "".join(f"<td>{esc(r.get(k,''))}</td>" for k, _ in cols) + "</tr>")
    table_html = (
        "<div class='table-wrap'><table><thead><tr>"
        + ths
        + "</tr></thead><tbody>"
        + ("".join(trs) if trs else "<tr><td colspan='8' class='muted'>Нет данных</td></tr>")
        + "</tbody></table></div>"
    )

    def bullets(items: Any) -> str:
        if not items or not isinstance(items, list):
            return "<div class='muted'>Нет</div>"
        return "<ul>" + "".join(f"<li>{esc(_as_text(x))}</li>" for x in items) + "</ul>"

    takeaways = vs.get("global_takeaways") or []
    plan = vs.get("baseline_30d_plan") or []
    return (
        "<div class='card'>"
        f"<div class='card-title'>Сравнение всех центров с {esc(baseline)} (baseline)</div>"
        "<div class='muted'>Здесь показаны относительные метрики конкурентов vs baseline и рекомендации для baseline, основанные на сильных сторонах конкурентов.</div>"
        "</div>"
        + "<div class='card'><div class='card-title'>Таблица сравнения (× к baseline)</div>"
        + table_html
        + "</div>"
        + "<div class='grid' style='margin-top:12px'>"
        + "<div class='card'><div class='card-title'>Ключевые выводы</div>"
        + bullets(takeaways)
        + "</div>"
        + "<div class='card'><div class='card-title'>План для baseline на 30 дней</div>"
        + bullets(plan)
        + "</div>"
        + "</div>"
    )


def _vectara_themes_section_html(vectara_themes: dict[str, Any], vectara_index_info: dict[str, Any]) -> str:
    def esc(s: Any) -> str:
        return html.escape("" if s is None else str(s))

    if not vectara_themes:
        return (
            "<div class='card'>"
            "<div class='card-title'>Vectara: тематический бенчмарк</div>"
            "<div class='muted'>Vectara не включена или нет данных.</div>"
            "</div>"
        )

    err = vectara_themes.get("error")
    if err:
        return (
            "<div class='card'>"
            "<div class='card-title'>Vectara: тематический бенчмарк</div>"
            f"<div class='muted'>Ошибка: <code>{esc(err)}</code></div>"
            "</div>"
        )

    cols = vectara_themes.get("columns") or []
    rows = vectara_themes.get("rows") or []
    if not isinstance(cols, list) or not isinstance(rows, list) or not cols:
        return (
            "<div class='card'>"
            "<div class='card-title'>Vectara: тематический бенчмарк</div>"
            "<div class='muted'>Нет данных.</div>"
            "</div>"
        )

    # render a curated subset (human-readable)
    show_cols = [
        ("theme", "тема"),
        ("leader_profile", "лидер"),
        ("leader_views_median", "лидер: views p50"),
        ("leader_likes_median", "лидер: likes p50"),
        ("baseline_profile", "baseline"),
        ("baseline_views_median", "baseline: views p50"),
        ("baseline_likes_median", "baseline: likes p50"),
        ("leader_examples", "примеры лидера"),
    ]

    def linkify_list(s: Any) -> str:
        raw = ("" if s is None else str(s)).strip()
        if not raw:
            return ""
        urls = [u.strip() for u in raw.split(";") if u.strip()]
        out = []
        for u in urls[:3]:
            u_esc = esc(u)
            out.append(f'<a href="{u_esc}" target="_blank" rel="noopener noreferrer">{u_esc}</a>')
        return "<div class='chips'>" + "".join(f"<span class='chip'>{x}</span>" for x in out) + "</div>"

    ths = "".join(f"<th>{esc(title)}</th>" for _, title in show_cols)
    trs = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        tds = []
        for key, _title in show_cols:
            if key == "leader_examples":
                tds.append(f"<td>{linkify_list(r.get(key,''))}</td>")
            else:
                tds.append(f"<td>{esc(r.get(key,''))}</td>")
        trs.append("<tr>" + "".join(tds) + "</tr>")

    idx_info = ""
    if isinstance(vectara_index_info, dict) and vectara_index_info:
        if vectara_index_info.get("error"):
            idx_info = f"<div class='muted'>Индексация: ошибка <code>{esc(vectara_index_info.get('error'))}</code></div>"
        else:
            idx_info = (
                "<div class='muted'>"
                f"Индексация: indexed={esc(vectara_index_info.get('indexed',''))}, skipped={esc(vectara_index_info.get('skipped',''))}"
                "</div>"
            )

    table_html = (
        "<div class='table-wrap'><table><thead><tr>"
        + ths
        + "</tr></thead><tbody>"
        + ("".join(trs) if trs else "<tr><td colspan='8' class='muted'>Нет данных</td></tr>")
        + "</tbody></table></div>"
    )
    return (
        "<div class='card'>"
        "<div class='card-title'>Vectara: тематический бенчмарк (семантический поиск)</div>"
        "<div class='muted'>Для каждой темы Vectara выбирает релевантные посты и показывает, у кого медианные метрики выше. Это помогает понять, где конкуренты сильнее именно по формату/смыслам.</div>"
        + idx_info
        + "</div>"
        + "<div class='card'><div class='card-title'>Таблица по темам</div>"
        + table_html
        + "</div>"
    )


def _ollama_chat_json(*, base_url: str, model: str, messages: list[dict[str, str]], timeout: int = 120) -> dict[str, Any]:
    url = base_url.rstrip("/") + "/api/chat"
    body = {
        "model": model,
        "messages": messages,
        "stream": False,
        "format": "json",
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


def _ollama_json_call(
    *,
    base_url: str,
    model: str,
    system: str,
    user_obj: dict[str, Any],
    required_keys: list[str],
    timeout: int,
    retries: int = 1,
) -> dict[str, Any]:
    """
    Делает вызов к Ollama и пытается получить JSON dict.
    Если ответ невалидный или не содержит required_keys — повторяет с более жёсткой инструкцией.
    """
    sys = system
    last: dict[str, Any] = {}
    for attempt in range(retries + 1):
        resp = _ollama_chat_json(
            base_url=base_url,
            model=model,
            messages=[{"role": "system", "content": sys}, {"role": "user", "content": json.dumps(user_obj, ensure_ascii=False)}],
            timeout=timeout,
        )
        content = ((resp.get("message") or {}).get("content") or "").strip()
        parsed = _parse_llm_json(content)
        if isinstance(parsed, dict) and all(k in parsed for k in required_keys):
            return parsed
        last = parsed if isinstance(parsed, dict) else {"value": parsed}
        sys = (
            system
            + "\n\nВАЖНО: верни ТОЛЬКО JSON объект без Markdown/комментариев/текста вокруг. "
            + "Проверь, что в JSON есть ключи: "
            + ", ".join(required_keys)
        )
    raise RuntimeError(
        "LLM returned invalid JSON (missing required keys). "
        f"required={required_keys}; last_keys={list(last.keys()) if isinstance(last, dict) else type(last)}"
    )


def _vectara_is_enabled(cfg: Config) -> bool:
    return bool(cfg.vectara_enabled and cfg.vectara_customer_id and cfg.vectara_corpus_key and cfg.vectara_api_key)


def _vectara_headers(cfg: Config) -> dict[str, str]:
    # Personal API key auth:
    # - customer-id: your customer id
    # - x-api-key: personal or service key
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "customer-id": str(cfg.vectara_customer_id),
        "x-api-key": str(cfg.vectara_api_key),
    }


def _vectara_doc_id(item_key: str) -> str:
    # Stable unique id per IG item.
    h = hashlib.sha1(item_key.encode("utf-8")).hexdigest()  # 40 chars
    return f"ig_{h}"


def _vectara_post_json(*, url: str, body: dict[str, Any], headers: dict[str, str], timeout: int = 30) -> dict[str, Any]:
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url=url, data=data, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = ""
        try:
            raw = e.read().decode("utf-8")
        except Exception:
            raw = ""
        # Do NOT include any credentials in error messages.
        raise RuntimeError(f"Vectara HTTPError {e.code}: {raw[:500]}") from e
    except Exception as e:
        raise RuntimeError(f"Vectara request failed: {e}") from e


def _vectara_get_json(*, url: str, headers: dict[str, str], timeout: int = 30) -> dict[str, Any]:
    req = urllib.request.Request(url=url, method="GET", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = ""
        try:
            raw = e.read().decode("utf-8")
        except Exception:
            raw = ""
        raise RuntimeError(f"Vectara HTTPError {e.code}: {raw[:500]}") from e
    except Exception as e:
        raise RuntimeError(f"Vectara request failed: {e}") from e


def _vectara_index_one(
    *,
    cfg: Config,
    doc_id: str,
    text: str,
    context: str,
    metadata: dict[str, Any],
    timeout: int = 30,
) -> None:
    """
    Index one IG post into Vectara corpus as a core document with 1 part.

    We try a couple of request shapes for compatibility with potential API changes.
    """
    base = cfg.vectara_base_url.rstrip("/")
    url = f"{base}/v2/corpora/{urllib.parse.quote(cfg.vectara_corpus_key)}/documents"
    headers = _vectara_headers(cfg)

    doc = {
        "id": doc_id,
        "metadata": metadata,
        "document_parts": [
            {
                "text": text or "",
                "context": context or "",
                "metadata": metadata,
            }
        ],
    }

    attempts: list[dict[str, Any]] = [
        {"type": "core", **doc},
        {"type": "core", "document": doc},
        {"type": "core", "core_document": doc},
    ]
    last_err: Optional[Exception] = None
    for body in attempts:
        try:
            _vectara_post_json(url=url, body=body, headers=headers, timeout=timeout)
            return
        except Exception as e:
            last_err = e
            # ALREADY_EXISTS or conflict should be treated as success for idempotency
            msg = str(e)
            if "409" in msg or "ALREADY_EXISTS" in msg or "already exists" in msg.lower():
                return
    raise RuntimeError(f"Vectara indexing failed: {last_err}")


def _vectara_sync_index(
    *,
    conn: sqlite3.Connection,
    cfg: Config,
    rows: list[tuple[Any, ...]],
    timeout: int = 30,
    sleep_seconds: float = 0.05,
) -> dict[str, Any]:
    """
    Incrementally index posts in `rows` into Vectara.
    Uses `vectara_index_cache` to avoid re-indexing.
    """
    if not _vectara_is_enabled(cfg):
        return {"enabled": False, "reason": "vectara not configured", "indexed": 0, "skipped": 0}
    if not cfg.vectara_index:
        return {"enabled": True, "reason": "vectara indexing disabled", "indexed": 0, "skipped": 0}

    # rows tuples: (profile_label, profile_url, kind, media_type, timestamp_utc, url, shortCode, likes, comments, views, caption)
    item_keys: list[str] = []
    for r in rows:
        key = str(r[5] or "").strip() or str(r[6] or "").strip()
        if key:
            item_keys.append(key)
    item_keys = list(dict.fromkeys(item_keys))  # preserve order, unique

    existing: set[str] = set()
    if item_keys:
        # SQLite has a limit on the number of host parameters. Keep it safe.
        chunk_size = 800
        for chunk in _chunked(item_keys, chunk_size):
            q = f"SELECT item_key FROM vectara_index_cache WHERE item_key IN ({','.join(['?'] * len(chunk))})"
            for (k,) in conn.execute(q, chunk).fetchall():
                existing.add(str(k))

    indexed = 0
    skipped = 0
    now = _utc_now().isoformat()
    for r in rows:
        profile_label = str(r[0] or "")
        profile_url = str(r[1] or "")
        kind = str(r[2] or "")
        media_type = str(r[3] or "")
        ts = str(r[4] or "")
        url = str(r[5] or "").strip()
        short_code = str(r[6] or "").strip()
        likes = _to_int(r[7])
        comments = _to_int(r[8])
        views = _to_int(r[9])
        caption = str(r[10] or "").replace("\n", " ").strip()
        if len(caption) > 800:
            caption = caption[:799] + "…"

        item_key = url or short_code
        if not item_key:
            skipped += 1
            continue
        if item_key in existing:
            skipped += 1
            continue

        doc_id = _vectara_doc_id(item_key)
        md = {
            "profile": profile_label,
            "profile_url": profile_url,
            "url": url or "",
            "short_code": short_code or "",
            "timestamp_utc": ts,
            "kind": kind,
            "media_type": media_type,
            "likes": (None if likes is None or likes < 0 else likes),
            "comments": (None if comments is None or comments < 0 else comments),
            "views": (None if views is None or views < 0 else views),
            "source": "instagram",
        }
        context = f"{profile_label} {kind} {media_type} {ts} {url}"
        # Always include profile in text to enable filtering-less retrieval.
        text = f"[{profile_label}] {caption}".strip()

        _vectara_index_one(cfg=cfg, doc_id=doc_id, text=text, context=context, metadata=md, timeout=timeout)
        conn.execute(
            """
            INSERT OR REPLACE INTO vectara_index_cache(item_key, vectara_doc_id, profile_label, profile_url, indexed_at_utc)
            VALUES(?, ?, ?, ?, ?)
            """,
            (item_key, doc_id, profile_label, profile_url, now),
        )
        conn.commit()
        indexed += 1
        if sleep_seconds:
            time.sleep(sleep_seconds)

    return {"enabled": True, "indexed": indexed, "skipped": skipped}


def _vectara_meta_to_dict(v: Any) -> dict[str, Any]:
    if v is None:
        return {}
    if isinstance(v, dict):
        return v
    if isinstance(v, list):
        out: dict[str, Any] = {}
        for it in v:
            if not isinstance(it, dict):
                continue
            k = it.get("name") or it.get("key") or it.get("field") or it.get("type")
            if not k:
                continue
            if "value" in it:
                out[str(k)] = it.get("value")
            elif "values" in it:
                out[str(k)] = it.get("values")
            else:
                # last resort: keep the dict
                out[str(k)] = it
        return out
    return {}


def _vectara_extract_results(resp: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Normalize Vectara query response to a list of:
    {text, score, doc_id, part_metadata, document_metadata}
    """
    if not isinstance(resp, dict):
        return []

    # Try a few common shapes
    candidates = []
    for path in [
        ("search_results",),
        ("results",),
        ("response", "results"),
        ("response", "search_results"),
        ("result", "results"),
        ("result", "search_results"),
    ]:
        cur: Any = resp
        ok = True
        for p in path:
            if not isinstance(cur, dict) or p not in cur:
                ok = False
                break
            cur = cur[p]
        if ok and isinstance(cur, list):
            candidates = cur
            break

    out = []
    for r in candidates:
        if not isinstance(r, dict):
            continue
        text = r.get("result_text") or r.get("text") or r.get("snippet") or ""
        score = r.get("score")
        doc_id = r.get("document_id") or r.get("doc_id") or r.get("id") or ""
        part_md = _vectara_meta_to_dict(r.get("part_metadata") or r.get("metadata"))
        doc_md = _vectara_meta_to_dict(r.get("document_metadata") or r.get("doc_metadata"))
        out.append({"text": text, "score": score, "doc_id": doc_id, "part_metadata": part_md, "document_metadata": doc_md, "raw": r})
    return out


def _vectara_query_simple(*, cfg: Config, query: str, limit: int = 20, timeout: int = 30) -> list[dict[str, Any]]:
    """
    Use the simple single-corpus endpoint (GET).
    Параметр запроса: `query` (см. GET /v2/corpora/:corpus_key/query).
    """
    if not _vectara_is_enabled(cfg):
        return []
    base = cfg.vectara_base_url.rstrip("/")
    corpus = urllib.parse.quote(cfg.vectara_corpus_key)
    headers = _vectara_headers(cfg)

    def call(params: dict[str, Any]) -> dict[str, Any]:
        qs = urllib.parse.urlencode(params)
        url = f"{base}/v2/corpora/{corpus}/query?{qs}"
        return _vectara_get_json(url=url, headers=headers, timeout=timeout)

    resp = call({"query": query, "limit": int(limit)})
    return _vectara_extract_results(resp)


def _vectara_theme_benchmark(
    *,
    cfg: Config,
    baseline_profile: str,
    limit: int,
) -> dict[str, Any]:
    """
    Semantic theme benchmark across all indexed posts.
    Returns dict with columns + rows for HTML/CSV.
    """
    themes = [
        {"theme": "Отзывы / доверие", "query": "отзыв пациента результат лечения впечатления благодарность"},
        {"theme": "Врач / экспертность", "query": "врач отвечает консультация экспертное мнение рекомендации"},
        {"theme": "Разбор случая", "query": "клинический случай разбор история пациента диагноз лечение"},
        {"theme": "Процедура: как проходит", "query": "как проходит процедура подготовка этапы реабилитация"},
        {"theme": "Цены / прозрачность", "query": "стоимость цена сколько стоит акция скидка рассрочка"},
        {"theme": "Оборудование / технологии", "query": "оборудование аппарат технология современное лечение"},
        {"theme": "Команда / закулисье", "query": "команда врачи клиника день из жизни закулисье"},
        {"theme": "Профилактика / полезное", "query": "профилактика советы симптомы когда обращаться"},
    ]

    rows: list[dict[str, Any]] = []
    for t in themes:
        q = t["query"]
        results = _vectara_query_simple(cfg=cfg, query=q, limit=limit)
        # group by profile (from metadata)
        by_prof: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for r in results:
            md = (r.get("document_metadata") or {}) | (r.get("part_metadata") or {})
            prof = str(md.get("profile") or "").strip()
            url = str(md.get("url") or "").strip()
            likes = _to_int(md.get("likes"))
            views = _to_int(md.get("views"))
            if likes is not None and likes < 0:
                likes = None
            if views is not None and views < 0:
                views = None
            if not prof:
                # fallback: try to infer profile from bracketed prefix in text
                txt = str(r.get("text") or "")
                m = re.match(r"\\[(.*?)\\]\\s+", txt)
                if m:
                    prof = m.group(1).strip()
            if not prof:
                continue
            by_prof[prof].append({"url": url, "likes": likes, "views": views})

        def median(vals: list[int]) -> Optional[float]:
            v = [x for x in vals if x is not None]
            return float(statistics.median(v)) if v else None

        stats: list[dict[str, Any]] = []
        for prof, items in by_prof.items():
            likes_vals = [x.get("likes") for x in items if x.get("likes") is not None]
            views_vals = [x.get("views") for x in items if x.get("views") is not None]
            stats.append(
                {
                    "profile": prof,
                    "n": len(items),
                    "likes_median": median(likes_vals),
                    "views_median": median(views_vals),
                    "examples": [x.get("url") for x in sorted(items, key=lambda z: (z.get("views") or 0, z.get("likes") or 0), reverse=True) if x.get("url")][:3],
                }
            )

        # select leader by views_median, fallback likes_median
        stats_sorted = sorted(
            stats,
            key=lambda x: (
                x.get("views_median") if x.get("views_median") is not None else -1,
                x.get("likes_median") if x.get("likes_median") is not None else -1,
                x.get("n") or 0,
            ),
            reverse=True,
        )
        leader = stats_sorted[0] if stats_sorted else {}
        baseline = next((s for s in stats if s.get("profile") == baseline_profile), {})

        rows.append(
            {
                "theme": t["theme"],
                "leader_profile": leader.get("profile", ""),
                "leader_n": leader.get("n", ""),
                "leader_views_median": leader.get("views_median", ""),
                "leader_likes_median": leader.get("likes_median", ""),
                "baseline_profile": baseline_profile,
                "baseline_n": baseline.get("n", ""),
                "baseline_views_median": baseline.get("views_median", ""),
                "baseline_likes_median": baseline.get("likes_median", ""),
                "leader_examples": "; ".join([u for u in (leader.get("examples") or []) if u]),
            }
        )

    columns = [
        "theme",
        "leader_profile",
        "leader_n",
        "leader_views_median",
        "leader_likes_median",
        "baseline_profile",
        "baseline_n",
        "baseline_views_median",
        "baseline_likes_median",
        "leader_examples",
    ]
    return {"columns": columns, "rows": rows}


def _write_vectara_themes_csv(path: Path, bench: dict[str, Any]) -> None:
    cols = bench.get("columns") or []
    rows = bench.get("rows") or []
    if not cols or not isinstance(cols, list):
        cols = []
    if not isinstance(rows, list):
        rows = []
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            if isinstance(r, dict):
                w.writerow({c: r.get(c, "") for c in cols})


def _write_llm_benchmark_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    cols = [
        "profile",
        "profile_url",
        "items_total",
        "posts_count",
        "reels_count",
        "posts_per_week",
        "media_mix",
        "likes_avg",
        "likes_median",
        "likes_p75",
        "likes_p90",
        "comments_avg",
        "views_avg",
        "views_median",
        "views_p75",
        "views_p90",
        "video_views_avg",
        "video_likes_avg",
        "positioning",
        "top_pillars",
        "strengths",
        "gaps",
        "priority_actions",
        "llm_error",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in cols})


def _write_vs_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    cols = [
        "competitor",
        "baseline",
        "posts_per_week_ratio",
        "likes_median_ratio",
        "likes_p75_ratio",
        "comments_avg_ratio",
        "views_median_ratio",
        "views_p75_ratio",
        "top_advantages",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in cols})


def _num(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, bool):
            return None
        return float(x)
    except Exception:
        s = str(x).strip().replace(",", ".")
        if not s:
            return None
        try:
            return float(s)
        except Exception:
            return None


def _ratio(a: Optional[float], b: Optional[float]) -> Optional[float]:
    # a / b
    if a is None or b in (None, 0.0):
        return None
    return a / b


def _top_posts_for_profile(all_rows: list[tuple[Any, ...]], profile: str, k: int = 2) -> dict[str, list[dict[str, Any]]]:
    # all_rows tuples: (profile_label, profile_url, kind, media_type, timestamp_utc, url, shortCode, likes, comments, views, caption)
    items = [r for r in all_rows if str(r[0] or "") == profile]

    def row_to_post(r):
        caption = str(r[10] or "").replace("\n", " ").strip()
        if len(caption) > 200:
            caption = caption[:199] + "…"
        return {
            "url": str(r[5] or ""),
            "timestamp_utc": str(r[4] or ""),
            "kind": str(r[2] or ""),
            "media_type": str(r[3] or ""),
            "likes": (lambda v: (None if (v is None or v < 0) else v))(_to_int(r[7])),
            "comments": (lambda v: (None if (v is None or v < 0) else v))(_to_int(r[8])),
            "views": (lambda v: (None if (v is None or v < 0) else v))(_to_int(r[9])),
            "caption": caption,
        }

    likes_sorted = sorted(items, key=lambda r: (_to_int(r[7]) if (_to_int(r[7]) is not None and _to_int(r[7]) >= 0) else -1), reverse=True)
    views_sorted = sorted(items, key=lambda r: (_to_int(r[9]) if (_to_int(r[9]) is not None and _to_int(r[9]) >= 0) else -1), reverse=True)
    return {
        "top_by_likes": [row_to_post(r) for r in likes_sorted[:k]],
        "top_by_views": [row_to_post(r) for r in views_sorted[:k]],
    }


def _pick_baseline_profile(bench_rows: list[dict[str, Any]], baseline: str) -> str:
    b = (baseline or "").strip().lower()
    if not bench_rows:
        return baseline
    # If user passed a URL - match by profile_url
    if b.startswith("http"):
        b_url = _normalize_profile_url(baseline)
        for r in bench_rows:
            if _normalize_profile_url(str(r.get("profile_url") or "")) == b_url:
                return str(r.get("profile") or baseline)
    # exact match first
    for r in bench_rows:
        p = str(r.get("profile") or "")
        if p.lower() == b:
            return p
    # match by profile_url substring (e.g. "kravira.by")
    for r in bench_rows:
        u = str(r.get("profile_url") or "").lower()
        if b and b in u:
            return str(r.get("profile") or baseline)
    # substring match
    for r in bench_rows:
        p = str(r.get("profile") or "")
        if b and b in p.lower():
            return p
    # default: first row
    return str(bench_rows[0].get("profile") or baseline)


def _pick_baseline_from_rows(rows: list[tuple[Any, ...]], baseline: str) -> str:
    """
    Pick baseline label from raw DB rows when benchmark rows are not available.
    rows tuples start with profile_label, profile_url.
    """
    b = (baseline or "").strip().lower()
    if not rows:
        return baseline
    labels = []
    urls = []
    for r in rows:
        labels.append(str(r[0] or ""))
        urls.append(str(r[1] or ""))
    # exact label match
    for lab in labels:
        if lab.lower() == b:
            return lab
    # match by URL substring
    for lab, u in zip(labels, urls):
        if b and b in (u or "").lower():
            return lab
    # substring label match
    for lab in labels:
        if b and b in lab.lower():
            return lab
    # default: first label
    return labels[0] or baseline


def _build_vs_baseline(
    *,
    base_url: str,
    model: str,
    timeout_seconds: int,
    baseline: str,
    bench_rows: list[dict[str, Any]],
    all_rows: list[tuple[Any, ...]],
    start_s: str,
    end_s: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """
    Строим сравнение всех центров с baseline (обычно kravira.by) и даём рекомендации для baseline,
    основанные на сильных сторонах конкурентов.
    """
    if not bench_rows:
        raise RuntimeError("Нет bench_rows для сравнения")

    baseline_profile = _pick_baseline_profile(bench_rows, baseline)
    idx = {str(r.get("profile") or ""): r for r in bench_rows}
    base = idx.get(baseline_profile)
    if not base:
        raise RuntimeError(f"Не найден baseline профиль '{baseline_profile}' в bench_rows")

    base_metrics = {
        "posts_per_week": _num(base.get("posts_per_week")),
        "likes_median": _num(base.get("likes_median")),
        "likes_p75": _num(base.get("likes_p75")),
        "comments_avg": _num(base.get("comments_avg")),
        "views_median": _num(base.get("views_median")),
        "views_p75": _num(base.get("views_p75")),
    }

    vs_rows: list[dict[str, Any]] = []
    for prof, r in idx.items():
        if not prof or prof == baseline_profile:
            continue
        m = {
            "posts_per_week": _num(r.get("posts_per_week")),
            "likes_median": _num(r.get("likes_median")),
            "likes_p75": _num(r.get("likes_p75")),
            "comments_avg": _num(r.get("comments_avg")),
            "views_median": _num(r.get("views_median")),
            "views_p75": _num(r.get("views_p75")),
        }
        ratios = {
            k: _ratio(m.get(k), base_metrics.get(k))
            for k in ("posts_per_week", "likes_median", "likes_p75", "comments_avg", "views_median", "views_p75")
        }
        # pick top advantages (only where competitor is clearly better than baseline)
        adv = sorted(
            [(k, v) for k, v in ratios.items() if v is not None and v >= 1.15],
            key=lambda t: t[1],
            reverse=True,
        )[:4]
        top_advantages = "; ".join([f"{k}×{v:.2f}" for k, v in adv if v is not None])

        def fmt(v: Optional[float]) -> str:
            return "" if v is None else f"{v:.2f}"

        vs_rows.append(
            {
                "competitor": prof,
                "baseline": baseline_profile,
                "posts_per_week_ratio": fmt(ratios.get("posts_per_week")),
                "likes_median_ratio": fmt(ratios.get("likes_median")),
                "likes_p75_ratio": fmt(ratios.get("likes_p75")),
                "comments_avg_ratio": fmt(ratios.get("comments_avg")),
                "views_median_ratio": fmt(ratios.get("views_median")),
                "views_p75_ratio": fmt(ratios.get("views_p75")),
                "top_advantages": top_advantages,
            }
        )

    # LLM: per-competitor (smaller prompts, more robust for small local models)
    baseline_posts = _top_posts_for_profile(all_rows, baseline_profile, k=2)
    comparisons: list[dict[str, Any]] = []
    for row in sorted(vs_rows, key=lambda x: x.get("top_advantages", ""), reverse=True):
        comp = row["competitor"]
        comp_posts = _top_posts_for_profile(all_rows, comp, k=2)
        # Recompute numeric advantages with baseline/competitor values (for LLM clarity)
        r_comp = idx.get(comp, {})
        m_comp = {
            "posts_per_week": _num(r_comp.get("posts_per_week")),
            "likes_median": _num(r_comp.get("likes_median")),
            "likes_p75": _num(r_comp.get("likes_p75")),
            "comments_avg": _num(r_comp.get("comments_avg")),
            "views_median": _num(r_comp.get("views_median")),
            "views_p75": _num(r_comp.get("views_p75")),
        }
        adv_list = []
        for key in ("views_p75", "views_median", "likes_p75", "likes_median", "comments_avg", "posts_per_week"):
            rr = _ratio(m_comp.get(key), base_metrics.get(key))
            if rr is not None and rr >= 1.15:
                adv_list.append(
                    {
                        "metric": key,
                        "baseline": base_metrics.get(key),
                        "competitor": m_comp.get(key),
                        "ratio": round(rr, 2),
                    }
                )

        system_comp = (
            "Ты senior performance маркетолог Instagram для медицинских центров. "
            "Сравни competitor с baseline и дай рекомендации ИМЕННО для baseline, чему научиться у competitor. "
            "Пиши по-русски, без хэштегов и без @упоминаний, без английских слов/фраз. "
            "Фокусируйся ТОЛЬКО на преимуществах competitor (advantages). Если advantages пуст — напиши, что явных преимуществ нет. "
            "Используй для примеров ТОЛЬКО URL постов из top_posts (не ссылку на профиль). "
            "Отвечай строго JSON: "
            '{"competitor":"","where_competitor_better":["..."],'
            '"what_they_do_better":["..."],'
            '"recommendations_for_baseline":[{"initiative":"","why":"","how":"","kpi":"","evidence_urls":["..."]}]}'
        )
        user_comp = {
            "period": f"{start_s}..{end_s} (UTC, end-exclusive)",
            "baseline": {"profile": baseline_profile, "benchmark": base, "top_posts": baseline_posts},
            "competitor": {"profile": comp, "benchmark": idx.get(comp, {}), "vs_ratios": row, "top_posts": comp_posts},
            "advantages": adv_list,
        }
        if not adv_list:
            comparisons.append(
                {
                    "competitor": comp,
                    "where_competitor_better": [],
                    "what_they_do_better": ["явных преимуществ по метрикам относительно baseline не найдено"],
                    "recommendations_for_baseline": [],
                }
            )
            continue
        try:
            comp_json = _ollama_json_call(
                base_url=base_url,
                model=model,
                system=system_comp,
                user_obj=user_comp,
                required_keys=["competitor", "where_competitor_better", "recommendations_for_baseline"],
                timeout=max(30, int(timeout_seconds)),
                retries=1,
            )
        except Exception as e:
            comp_json = {"competitor": comp, "error": str(e), "where_competitor_better": [], "what_they_do_better": [], "recommendations_for_baseline": []}
        # Post-process: ensure competitor name and at least some structured fields are present.
        if not (comp_json.get("competitor") or "").strip():
            comp_json["competitor"] = comp
        # Prefer our computed advantages (less hallucination-prone).
        if adv_list:
            comp_json["where_competitor_better"] = [f"{a['metric']}×{a['ratio']}" for a in adv_list]
        recs = comp_json.get("recommendations_for_baseline")
        if not isinstance(recs, list):
            recs = []
        top_urls = []
        try:
            top_urls = [p.get("url") for p in (comp_posts.get("top_by_views") or []) if isinstance(p, dict) and p.get("url")] + [
                p.get("url") for p in (comp_posts.get("top_by_likes") or []) if isinstance(p, dict) and p.get("url")
            ]
            top_urls = [u for u in top_urls if u]
        except Exception:
            top_urls = []
        for r in recs:
            if not isinstance(r, dict):
                continue
            if not (r.get("kpi") or "").strip() and adv_list:
                r["kpi"] = adv_list[0]["metric"]
            ev = r.get("evidence_urls")
            valid_posts: list[str] = []
            if isinstance(ev, list):
                for x in ev:
                    u = str(x or "").strip()
                    if u and ("/p/" in u or "/reel/" in u):
                        valid_posts.append(u)
            if not valid_posts:
                valid_posts = top_urls[:3]
            r["evidence_urls"] = valid_posts
        comp_json["recommendations_for_baseline"] = recs
        comparisons.append(comp_json)

    # Synthesize baseline plan (short prompt)
    system_plan = (
        "Ты senior performance маркетолог. "
        "На входе сравнения baseline с конкурентами и их рекомендации. "
        "Собери общий план для baseline на 30 дней (P1..P3) и ключевые выводы. "
        "Пиши по-русски, без хэштегов и без @упоминаний, без английских слов/фраз. "
        "Отвечай строго JSON: "
        '{"baseline_30d_plan":[{"priority":"P1","initiative":"","hypothesis":"","test":"","success_metric":"","expected_impact":""}],"global_takeaways":["..."]}'
    )
    try:
        plan_json = _ollama_json_call(
            base_url=base_url,
            model=model,
            system=system_plan,
            user_obj={"baseline": baseline_profile, "comparisons": comparisons},
            required_keys=["baseline_30d_plan", "global_takeaways"],
            timeout=max(30, int(timeout_seconds)),
            retries=1,
        )
    except Exception as e:
        plan_json = {"baseline_30d_plan": [], "global_takeaways": [], "error": str(e)}

    vs_payload = {
        "baseline": baseline_profile,
        "vs_table": vs_rows,
        "comparisons": comparisons,
        "baseline_30d_plan": plan_json.get("baseline_30d_plan") or [],
        "global_takeaways": plan_json.get("global_takeaways") or [],
    }
    # Fallback: if the model returned an empty plan, build a minimal one from extracted recommendations.
    plan = vs_payload.get("baseline_30d_plan") or []
    if not isinstance(plan, list) or not any(isinstance(x, dict) and (x.get("initiative") or "").strip() for x in plan):
        recs: list[dict[str, Any]] = []
        for c in comparisons:
            for r in (c.get("recommendations_for_baseline") or [])[:2]:
                if isinstance(r, dict):
                    recs.append(r)
        built = []
        for i, r in enumerate(recs[:3], start=1):
            built.append(
                {
                    "priority": f"P{i}",
                    "initiative": (r.get("initiative") or "").strip(),
                    "hypothesis": (r.get("why") or "").strip(),
                    "test": (r.get("how") or "").strip(),
                    "success_metric": (r.get("kpi") or "").strip(),
                    "expected_impact": "рост метрики по выбранному KPI",
                }
            )
        vs_payload["baseline_30d_plan"] = built
        if not vs_payload.get("global_takeaways"):
            vs_payload["global_takeaways"] = ["План собран из сильных сторон конкурентов; см. таблицу преимуществ и примеры постов."]
    return vs_payload, vs_rows


def _build_llm_insights(
    *,
    rows: list[tuple[Any, ...]],
    start_s: str,
    end_s: str,
    model: str,
    base_url: str,
    chunk_size: int,
    max_posts_per_profile: int,
    timeout_seconds: int,
    polish: bool,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """
    Профессиональный LLM-анализ по ВСЕМ постам (чанками) + сравнительный бенчмарк.
    Возвращает (llm_json_for_report, benchmark_rows_for_csv).
    """
    # group rows by profile_label
    by_profile: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
    for r in rows:
        by_profile[str(r[0] or "")].append(r)

    profile_outputs: list[dict[str, Any]] = []
    benchmark_rows: list[dict[str, Any]] = []

    # per-profile analysis
    for profile, items in sorted(by_profile.items()):
        # normalize items into dicts
        posts = []
        likes_vals: list[int] = []
        comments_vals: list[int] = []
        views_vals: list[int] = []
        posts_count = 0
        reels_count = 0
        media_counts: dict[str, int] = defaultdict(int)
        video_views: list[int] = []
        video_likes: list[int] = []
        ts_vals: list[str] = []

        for r in items:
            # (profile_label, profile_url, content_kind, media_type, timestamp_utc, url, short_code, likes, comments, views, caption)
            profile_url = str(r[1] or "")
            kind = str(r[2] or "")
            media_type = str(r[3] or "")
            ts = str(r[4] or "")
            url = str(r[5] or "")
            likes = _to_int(r[7])
            comments = _to_int(r[8])
            views = _to_int(r[9])
            # Some scrapers return -1 when a metric is hidden/unavailable.
            if likes is not None and likes < 0:
                likes = None
            if comments is not None and comments < 0:
                comments = None
            if views is not None and views < 0:
                views = None
            # For LLM: keep captions short to fit context even with many posts.
            caption = str(r[10] or "").replace("\n", " ").strip()
            if len(caption) > 160:
                caption = caption[:159] + "…"

            if kind == "reel":
                reels_count += 1
            elif kind == "post":
                posts_count += 1

            if likes is not None:
                likes_vals.append(likes)
            if comments is not None:
                comments_vals.append(comments)
            if views is not None:
                views_vals.append(views)
            if ts:
                ts_vals.append(ts)

            if media_type:
                media_counts[media_type] = int(media_counts.get(media_type, 0)) + 1
            if media_type.lower() == "video":
                if views is not None:
                    video_views.append(views)
                if likes is not None:
                    video_likes.append(likes)

            posts.append(
                {
                    "timestamp_utc": ts,
                    "url": url,
                    "kind": kind,
                    "media_type": media_type,
                    "likes": likes,
                    "comments": comments,
                    "views": views,
                    "caption": caption,
                }
            )

        # sort posts by time desc (input already sorted, but keep deterministic)
        posts = sorted(posts, key=lambda x: x.get("timestamp_utc") or "", reverse=True)
        if max_posts_per_profile and max_posts_per_profile > 0:
            posts = posts[: max_posts_per_profile]

        days = 0
        try:
            if ts_vals:
                tmax = _parse_iso_dt(max(ts_vals))
                tmin = _parse_iso_dt(min(ts_vals))
                days = max(1, int((tmax - tmin).total_seconds() // 86400) + 1)
        except Exception:
            days = 0

        items_total = len(posts)
        likes_avg = round((sum(likes_vals) / len(likes_vals)), 2) if likes_vals else ""
        comments_avg = round((sum(comments_vals) / len(comments_vals)), 2) if comments_vals else ""
        views_avg = round((sum(views_vals) / len(views_vals)), 2) if views_vals else ""

        # top examples (give LLM evidence)
        top_by_likes = sorted([p for p in posts if p.get("likes") is not None], key=lambda x: x.get("likes") or 0, reverse=True)[:5]
        top_by_views = sorted([p for p in posts if p.get("views") is not None], key=lambda x: x.get("views") or 0, reverse=True)[:5]

        kpis = {
            "profile": profile,
            "profile_url": (str(items[0][1] or "") if items else ""),
            "items_total": items_total,
            "posts_count": posts_count,
            "reels_count": reels_count,
            "posts_per_week": (round(items_total / days * 7.0, 2) if days else ""),
            "media_mix": dict(media_counts),
            "likes_avg": likes_avg,
            "likes_median": _median(likes_vals),
            "likes_p75": _percentile(likes_vals, 75),
            "likes_p90": _percentile(likes_vals, 90),
            "comments_avg": comments_avg,
            "views_avg": views_avg,
            "views_median": _median(views_vals),
            "views_p75": _percentile(views_vals, 75),
            "views_p90": _percentile(views_vals, 90),
            "video_views_avg": (round(sum(video_views) / len(video_views), 2) if video_views else ""),
            "video_likes_avg": (round(sum(video_likes) / len(video_likes), 2) if video_likes else ""),
        }

        # LLM pipeline (per-profile). Failures shouldn't kill the whole run.
        try:
            chunks = _chunked(posts, max(5, int(chunk_size)))
            chunk_summaries: list[dict[str, Any]] = []
            for idx, ch in enumerate(chunks, start=1):
                system = (
                    "Ты маркетолог-аналитик Instagram (медицинские центры). "
                    "Проанализируй контент-пиллары, позиционирование, аудиторию, CTA, "
                    "и признаки того, что влияет на метрики. "
                    "Пиши по-русски, профессионально и кратко. "
                    "Запрещено: хэштеги (#...), упоминания (@...), английские слова/фразы (если это не название бренда). "
                    "Если не уверен — верни пустые массивы, но ключи JSON обязаны быть. "
                    "Отвечай СТРОГО JSON объектом вида: "
                    '{"pillars":[{"pillar":"","intent":"","audience":"","count_est":0,"examples":["url1","url2"]}],'
                    '"drivers":["..."],"gaps":["..."],"strengths":["..."],'
                    '"cta_patterns":["..."],"compliance_risks":["..."],"notes":["..."]}'
                )
                user = {
                    "profile": profile,
                    "period": f"{start_s}..{end_s} (UTC, end-exclusive)",
                    "chunk": {"index": idx, "of": len(chunks)},
                    "posts": ch,
                    "kpis_hint": kpis,
                }
                chunk_summaries.append(
                    _ollama_json_call(
                        base_url=base_url,
                        model=model,
                        system=system,
                        user_obj=user,
                        required_keys=["pillars", "drivers", "gaps", "strengths"],
                        timeout=max(30, int(timeout_seconds)),
                        retries=2,
                    )
                )

            system_final = (
                "Ты маркетолог-аналитик. У тебя есть KPI профиля и чанковые выводы по всем постам. "
                "Собери профессиональный итог: позиционирование, аудиторию, контент-пиллары (с долей), "
                "драйверы метрик, сильные стороны, пробелы/риски, и план тестов на 30 дней (P1..P3). "
                "Опирайся на KPI и примеры top-постов (URL) и формат (media_type). "
                "Запрещено: хэштеги (#...), упоминания (@...), английские слова/фразы (если это не название бренда). "
                "Если не уверен — заполни ключи максимально безопасно (пустые массивы/пустые строки), "
                "но ключи JSON обязаны быть. "
                "Отвечай строго JSON объектом вида: "
                '{"positioning":"","audience":["..."],'
                '"content_pillars":[{"pillar":"","share_pct":0,"best_formats":["reel","post"],"evidence":"","example_urls":["..."]}],'
                '"performance_drivers":["..."],"strengths":["..."],"gaps":["..."],'
                '"action_plan_30d":[{"priority":"P1","initiative":"","hypothesis":"","test":"","success_metric":"","expected_impact":""}]}'
            )
            user_final = {
                "profile": profile,
                "period": f"{start_s}..{end_s} (UTC, end-exclusive)",
                "kpis": kpis,
                "top_posts_by_likes": top_by_likes,
                "top_posts_by_views": top_by_views,
                "chunk_summaries": chunk_summaries,
            }
            parsed = _ollama_json_call(
                base_url=base_url,
                model=model,
                system=system_final,
                user_obj=user_final,
                required_keys=["positioning", "content_pillars", "action_plan_30d", "strengths", "gaps"],
                timeout=max(30, int(timeout_seconds)),
                retries=2,
            )

            if polish:
                system_polish = (
                    "Ты редактор и стратег. Приведи результат к профессиональному русскому языку: "
                    "убери англицизмы, хэштеги, @упоминания. Заполни пропуски разумно на основе входных данных. "
                    "Сохрани структуру и ключи JSON 1-в-1."
                )
                parsed = _ollama_json_call(
                    base_url=base_url,
                    model=model,
                    system=system_polish,
                    user_obj={
                        "profile": profile,
                        "kpis": kpis,
                        "top_posts_by_likes": top_by_likes,
                        "top_posts_by_views": top_by_views,
                        "draft": parsed,
                    },
                    required_keys=["positioning", "content_pillars", "action_plan_30d", "strengths", "gaps"],
                    timeout=max(30, int(timeout_seconds)),
                    retries=1,
                )
        except Exception as e:
            parsed = {"positioning": "", "audience": [], "content_pillars": [], "performance_drivers": [], "strengths": [], "gaps": [], "action_plan_30d": [], "error": str(e)}

        prof_out = {
            "profile": profile,
            "kpis": kpis,
            "positioning": parsed.get("positioning") or "",
            "audience": parsed.get("audience") or [],
            "content_pillars": parsed.get("content_pillars") or [],
            "performance_drivers": parsed.get("performance_drivers") or [],
            "strengths": parsed.get("strengths") or [],
            "gaps": parsed.get("gaps") or [],
            "action_plan_30d": parsed.get("action_plan_30d") or [],
            "notes": (parsed.get("notes") or []) + (["views есть не у всех постов"] if not views_vals else []),
            "llm_error": parsed.get("error") or "",
        }
        profile_outputs.append(prof_out)

        # benchmark row (for CSV)
        top_pillars = ""
        try:
            cps = prof_out.get("content_pillars") or []
            if isinstance(cps, list):
                top_pillars = "; ".join([_as_text(x.get("pillar") if isinstance(x, dict) else x) for x in cps[:5] if _as_text(x.get("pillar") if isinstance(x, dict) else x)])
        except Exception:
            top_pillars = ""
        gaps = _join_list(prof_out.get("gaps"), 6)
        strengths = _join_list(prof_out.get("strengths"), 6)
        actions = ""
        try:
            ap = prof_out.get("action_plan_30d") or []
            if isinstance(ap, list):
                parts: list[str] = []
                for x in ap[:5]:
                    if isinstance(x, dict):
                        parts.append(f"{_as_text(x.get('priority'))}: {_as_text(x.get('initiative'))}".strip(": ").strip())
                    else:
                        parts.append(_as_text(x))
                actions = "; ".join([p for p in parts if p])
        except Exception:
            actions = ""

        benchmark_rows.append(
            {
                "profile": profile,
                "profile_url": kpis.get("profile_url", ""),
                "items_total": kpis.get("items_total", ""),
                "posts_count": kpis.get("posts_count", ""),
                "reels_count": kpis.get("reels_count", ""),
                "posts_per_week": kpis.get("posts_per_week", ""),
                "media_mix": json.dumps(kpis.get("media_mix") or {}, ensure_ascii=False),
                "likes_avg": kpis.get("likes_avg", ""),
                "likes_median": kpis.get("likes_median", ""),
                "likes_p75": kpis.get("likes_p75", ""),
                "likes_p90": kpis.get("likes_p90", ""),
                "comments_avg": kpis.get("comments_avg", ""),
                "views_avg": kpis.get("views_avg", ""),
                "views_median": kpis.get("views_median", ""),
                "views_p75": kpis.get("views_p75", ""),
                "views_p90": kpis.get("views_p90", ""),
                "video_views_avg": kpis.get("video_views_avg", ""),
                "video_likes_avg": kpis.get("video_likes_avg", ""),
                "positioning": prof_out.get("positioning", ""),
                "top_pillars": top_pillars,
                "strengths": strengths,
                "gaps": gaps,
                "priority_actions": actions,
                "llm_error": prof_out.get("llm_error", ""),
            }
        )

    # global benchmark in llm json (for HTML)
    columns = [
        "profile",
        "items_total",
        "posts_per_week",
        "likes_avg",
        "views_avg",
        "positioning",
        "top_pillars",
        "priority_actions",
    ]
    bench_table_rows = [{c: r.get(c, "") for c in columns} for r in benchmark_rows]

    out = {
        "provider": "ollama",
        "base_url": base_url,
        "model": model,
        "period": {"start": start_s, "end": end_s},
        "benchmark": {"columns": columns, "rows": bench_table_rows},
        "profiles": profile_outputs,
    }
    return out, benchmark_rows


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
    llm_chunk_size = int(getattr(args, "llm_chunk_size", None) or (os.environ.get("LLM_CHUNK_SIZE") or 30))
    llm_max_posts = int(getattr(args, "llm_max_posts", None) or (os.environ.get("LLM_MAX_POSTS_PER_PROFILE") or 0))
    ollama_timeout = int(getattr(args, "ollama_timeout", None) or (os.environ.get("OLLAMA_TIMEOUT_SECONDS") or 180))
    llm_polish = bool(getattr(args, "llm_polish", False) or (os.environ.get("LLM_POLISH") or "").strip().lower() in {"1", "true", "yes", "on"})
    baseline_profile = (getattr(args, "baseline", None) or (os.environ.get("BASELINE_PROFILE") or "kravira")).strip()

    # Vectara (optional)
    vectara_base_url = (os.environ.get("VECTARA_BASE_URL") or "https://api.vectara.io").strip()
    vectara_customer_id = (getattr(args, "vectara_customer_id", None) or (os.environ.get("VECTARA_CUSTOMER_ID") or "")).strip()
    vectara_corpus_key = (getattr(args, "vectara_corpus", None) or (os.environ.get("VECTARA_CORPUS_KEY") or "")).strip()
    vectara_api_key = (os.environ.get("VECTARA_API_KEY") or "").strip()
    vectara_theme_limit = int(getattr(args, "vectara_theme_limit", None) or (os.environ.get("VECTARA_THEME_LIMIT") or 40))

    vectara_enabled_flag = getattr(args, "vectara", None)
    vectara_disabled_flag = getattr(args, "no_vectara", None)
    vectara_enabled = False
    if vectara_enabled_flag:
        vectara_enabled = True
    elif vectara_disabled_flag:
        vectara_enabled = False
    else:
        # auto: enable only if all required vars are present
        vectara_enabled = bool(vectara_customer_id and vectara_corpus_key and vectara_api_key)
    vectara_index = bool(getattr(args, "vectara_index", False) or (os.environ.get("VECTARA_INDEX") or "").strip().lower() in {"1", "true", "yes", "on"})

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
        llm_chunk_size=max(5, llm_chunk_size),
        llm_max_posts_per_profile=max(0, llm_max_posts),
        ollama_timeout_seconds=max(30, ollama_timeout),
        llm_polish=llm_polish,
        baseline_profile=baseline_profile,
        vectara_base_url=vectara_base_url,
        vectara_customer_id=vectara_customer_id,
        vectara_api_key=vectara_api_key,
        vectara_corpus_key=vectara_corpus_key,
        vectara_enabled=vectara_enabled,
        vectara_index=vectara_index or vectara_enabled,  # default: index when enabled
        vectara_theme_limit=max(5, vectara_theme_limit),
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
    parser.add_argument("--llm-chunk-size", dest="llm_chunk_size", type=int, default=30, help="Сколько постов отправлять в LLM за 1 запрос (по умолчанию 30)")
    parser.add_argument("--llm-max-posts", dest="llm_max_posts", type=int, default=0, help="Ограничение постов на профиль для LLM (0 = все)")
    parser.add_argument("--ollama-timeout", dest="ollama_timeout", type=int, default=180, help="Таймаут одного запроса к Ollama (сек)")
    parser.add_argument("--llm-polish", dest="llm_polish", action="store_true", help="Доп. проход редактора (может быть медленнее)")
    parser.add_argument("--baseline", default=os.environ.get("BASELINE_PROFILE", "kravira"), help="С кем сравнивать всех (по умолчанию kravira)")
    g = parser.add_mutually_exclusive_group()
    g.add_argument("--vectara", action="store_true", help="Включить Vectara (поиск/бенчмарк по темам). По умолчанию auto если заданы env.")
    g.add_argument("--no-vectara", dest="no_vectara", action="store_true", help="Отключить Vectara даже если env заданы.")
    parser.add_argument("--vectara-corpus", dest="vectara_corpus", default=None, help="Vectara corpus_key (например crp_11). Можно задать через env VECTARA_CORPUS_KEY.")
    parser.add_argument("--vectara-customer-id", dest="vectara_customer_id", default=None, help="Vectara customer-id. Можно задать через env VECTARA_CUSTOMER_ID.")
    parser.add_argument("--vectara-index", dest="vectara_index", action="store_true", help="Принудительно индексировать новые посты в Vectara (иначе auto при включении).")
    parser.add_argument("--vectara-theme-limit", dest="vectara_theme_limit", type=int, default=40, help="Сколько результатов брать из Vectara на одну тему (по умолчанию 40)")
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

