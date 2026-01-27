#!/usr/bin/env python3
"""
Скрапинг прайса mercimed.by через Apify Puppeteer Scraper.
Результат — таблица по специальностям и красивый HTML-отчёт (без LLM).
"""
from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.apify.com"
PUPPETEER_ACTOR = "apify~puppeteer-scraper"
# Страница: ждём появления "BYN" или "руб" в body (до 20 сек), затем ищем прайс. Результат — через Apify.pushData (и return для совместимости).
PAGE_FUNCTION = r"""
async function pageFunction(context) {
  const { page, request, Apify } = context;
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));
  await sleep(6000);
  try {
    await page.waitForFunction(
      () => {
        const t = document.body && document.body.innerText ? document.body.innerText : '';
        return t.length > 4000 && (/BYN|руб|от\s+\d/i.test(t) || /\d[\d\s.,]+\s*(?:BYN|руб)/i.test(t));
      },
      { timeout: 25000 }
    );
    await sleep(4000);
  } catch (e) {}
  const url = request.url;
  const path = url.replace(/^https?:\/\/[^/]+/, '').replace(/\/$/, '');
  const specialty = path.split('/').filter(Boolean).pop() || 'main';
  const items = await page.evaluate(() => {
    const out = [];
    const seen = new Set();
    function add(name, price) {
      if (!name || name.length < 4) return;
      const key = (name.slice(0,100) + (price != null ? String(price) : '')).slice(0,120);
      if (seen.has(key)) return;
      seen.add(key);
      out.push({ name: name.trim(), price });
    }
    document.querySelectorAll('table tr').forEach(tr => {
      const cells = tr.querySelectorAll('td, th');
      if (cells.length >= 2) {
        const texts = Array.from(cells).map(c => c.innerText.trim()).filter(Boolean);
        let name = '', price = null;
        for (const t of texts) {
          if (/BYN|руб|р\.|бел/i.test(t)) {
            const m = t.match(/[\d\s.,]+/);
            if (m) price = parseFloat(m[0].replace(/\s/g,'').replace(',','.')) || null;
          } else if (/^\d[\d\s.,]*$/.test(t)) {
            if (price == null) price = parseFloat(t.replace(/\s/g,'').replace(',','.')) || null;
          } else {
            name = (name ? name + ' ' : '') + t;
          }
        }
        if (name) add(name, price);
      }
    });
    const bodyText = document.body && document.body.innerText ? document.body.innerText : '';
    const lines = bodyText.split(/\n+/);
    for (const line of lines) {
      const m = line.match(/^(.+?)\s+(?:от\s+)?(\d[\d\s.,]+)\s*(?:BYN|руб|р\.|бел\.?руб)/i);
      if (m) {
        const name = m[1].trim().replace(/^[—:\-\s•]+/, '').trim();
        const price = parseFloat(m[2].replace(/\s/g,'').replace(',','.'));
        if (name.length >= 4 && price >= 1 && price <= 999999) add(name, price);
      }
    }
    const fullRegex = /([^\n]{8,150}?)\s+(?:от\s+)?(\d[\d\s.,]+)\s*(?:BYN|руб|р\.|бел\.?руб)/gi;
    let mm;
    while ((mm = fullRegex.exec(bodyText)) !== null) {
      const name = mm[1].trim().replace(/^[—:\-\s•]+/, '').trim();
      const price = parseFloat(mm[2].replace(/\s/g,'').replace(',','.'));
      if (name.length >= 4 && price >= 1 && price <= 999999) add(name, price);
    }
    document.querySelectorAll('p, li, div, td, span, [class]').forEach(el => {
      const t = (el.innerText || '').trim();
      if (t.length < 8 || t.length > 600) return;
      const m = t.match(/(.+?)\s+(?:от\s+)?(\d[\d\s.,]{1,})\s*(?:BYN|руб|р\.|бел\.?руб)/i);
      if (m) {
        const name = m[1].trim().replace(/^[—:\-\s•]+/, '').trim();
        const price = parseFloat(m[2].replace(/\s/g,'').replace(',','.'));
        if (name.length >= 4 && price >= 1 && price <= 999999) add(name, price);
      }
    });
    return out;
  });
  const record = { url, specialty, items };
  if (typeof Apify !== 'undefined' && Apify.pushData) {
    await Apify.pushData(record);
  }
  return record;
}
"""


def _apify_request(
    *,
    base_url: str,
    token: str,
    method: str,
    path: str,
    body: dict[str, Any] | None = None,
    timeout: int = 90,
) -> dict[str, Any]:
    q = {"token": token}
    url = base_url.rstrip("/") + "/" + path.lstrip("/") + "?" + urllib.parse.urlencode(q)
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json; charset=utf-8"
    req = urllib.request.Request(url=url, data=data, method=method.upper(), headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
        raise RuntimeError(f"Apify HTTP {e.code} {e.reason}: {raw}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Apify error: {e}") from e


def run_puppeteer_scraper(
    start_urls: list[str],
    token: str,
    base_url: str = BASE_URL,
    wait_timeout: int = 300,
    crawl_subpages: bool = True,
    max_pages: int = 50,
) -> list[dict[str, Any]]:
    """Запуск Apify Puppeteer Scraper. start_urls — список стартовых URL.
    Если crawl_subpages=True и один URL, обходит все ссылки под ним (globs = url + /**).
    """
    urls = [u.strip() for u in start_urls if u and u.strip()]
    if not urls:
        raise ValueError("Нужен хотя бы один start URL")
    run_input: dict[str, Any] = {
        "startUrls": [{"url": u} for u in urls],
        "pageFunction": PAGE_FUNCTION,
        "proxyConfiguration": {"useApifyProxy": True},
        "maxPagesPerCrawl": max_pages if crawl_subpages else len(urls),
        "pageFunctionTimeoutSecs": 150,
        "pageLoadTimeoutSecs": 90,
    }
    if crawl_subpages and len(urls) <= 1:
        base = urls[0].rstrip("/")
        run_input["linkSelector"] = "a[href]"
        run_input["globs"] = [base + "/**", base]
    started = _apify_request(
        base_url=base_url,
        token=token,
        method="POST",
        path=f"v2/acts/{PUPPETEER_ACTOR}/runs",
        body=run_input,
        timeout=90,
    )
    run = (started.get("data") or {}).get("id")
    if not run:
        raise RuntimeError(f"Apify: не получили run id: {started}")

    deadline = time.time() + wait_timeout
    run_data: dict[str, Any] = {}
    while time.time() < deadline:
        info = _apify_request(
            base_url=base_url,
            token=token,
            method="GET",
            path=f"v2/actor-runs/{run}",
            timeout=60,
        )
        data = info.get("data") or {}
        run_data = data
        status = (data.get("status") or "").upper()
        if status == "SUCCEEDED":
            ds_id = data.get("defaultDatasetId")
            if not ds_id:
                raise RuntimeError("Apify run без defaultDatasetId")
            break
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            raise RuntimeError(f"Apify run завершился: {status}")
        time.sleep(3)
    else:
        raise RuntimeError(f"Apify run не завершился за {wait_timeout}s")

    items: list[dict[str, Any]] = []
    offset = 0
    while True:
        u = base_url.rstrip("/") + f"/v2/datasets/{ds_id}/items?token={token}&format=json&offset={offset}&limit=100"
        req = urllib.request.Request(u, method="GET", headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=60) as r:
            raw = r.read().decode("utf-8")
            chunk = json.loads(raw) if raw else []
        if isinstance(chunk, dict):
            chunk = chunk.get("data") or chunk.get("items") or []
        if not isinstance(chunk, list):
            chunk = []
        if not chunk:
            break
        items.extend(chunk)
        offset += len(chunk)
    return items


def by_specialty_from_apify_items(apify_items: list[dict[str, Any]]) -> dict[str, list[tuple[str, float | None]]]:
    """Собрать по специальностям списки (название, цена) из вывода Apify."""
    by_spec: dict[str, list[tuple[str, float | None]]] = defaultdict(list)
    seen_per_spec: dict[str, set[str]] = defaultdict(set)
    skip_names = {"услуга стоимость", "услуга", "стоимость", "наименование", "название"}
    for row in apify_items:
        if row.get("#error"):
            continue
        spec = (row.get("specialty") or "main").strip()
        for it in row.get("items") or []:
            name = (it.get("name") or "").strip()
            if not name or len(name) < 5:
                continue
            if name.lower() in skip_names or re.match(r"^[\s\W]*$", name):
                continue
            p = it.get("price")
            price = float(p) if p is not None and (isinstance(p, (int, float)) or (isinstance(p, str) and re.match(r"[\d.,]+", str(p)))) else None
            key = (name[:80], price)
            if key in seen_per_spec[spec]:
                continue
            seen_per_spec[spec].add(key)
            by_spec[spec].append((name, price))
    return dict(by_spec)


def analysis_rows(by_specialty: dict[str, list[tuple[str, float | None]]]) -> list[dict[str, Any]]:
    """Анализ по специальностям: количество, мин/макс/средняя."""
    out = []
    for spec in sorted(by_specialty.keys()):
        items = by_specialty[spec]
        prices = [p for _, p in items if p is not None and p > 0]
        row = {
            "specialty": spec,
            "count": len(items),
            "with_price": len(prices),
            "min": round(min(prices), 2) if prices else None,
            "max": round(max(prices), 2) if prices else None,
            "avg": round(sum(prices) / len(prices), 2) if prices else None,
        }
        out.append(row)
    return out


def write_html_report(
    path: Path,
    by_specialty: dict[str, list[tuple[str, float | None]]],
    analysis: list[dict[str, Any]],
    source_url: str,
) -> None:
    """Пишет один HTML-файл с таблицами по специальностям и анализом."""
    def esc(s: Any) -> str:
        return html.escape(str(s) if s is not None else "")

    rows_html = []
    for spec in sorted(by_specialty.keys()):
        items = by_specialty[spec]
        if not items:
            continue
        rows_html.append(f"""
        <section class="card">
          <h2 class="card-title">{esc(spec)}</h2>
          <div class="table-wrap">
            <table>
              <thead><tr><th>Услуга / операция</th><th class="num">Цена, руб</th></tr></thead>
              <tbody>
                {"".join(
                  f"<tr><td>{esc(n)}</td><td class='num'>{esc(p) if p is not None else '—'}</td></tr>"
                  for n, p in items[:200]
                )}
              </tbody>
            </table>
          </div>
          {f"<p class='muted'>Показано до 200 записей, всего {len(items)}</p>" if len(items) > 200 else ""}
        </section>""")

    anal_tr = "".join(
        f"<tr><td>{esc(r['specialty'])}</td><td class='num'>{r['count']}</td><td class='num'>{r['with_price']}</td>"
        f"<td class='num'>{esc(r['min'])}</td><td class='num'>{esc(r['max'])}</td><td class='num'>{esc(r['avg'])}</td></tr>"
        for r in analysis
    ) if analysis else "<tr><td colspan='6' class='muted'>По текущим селекторам данных не извлечено. Возможно, структура страницы изменилась — откройте страницу в браузере и проверьте блоки с прайсом.</td></tr>"

    doc = f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Прайс Mercimed — по специальностям</title>
  <style>
    :root {{
      --bg: #0b1220;
      --card: #111a2e;
      --text: #e9eefc;
      --muted: #9fb0d0;
      --accent: #7aa2ff;
      --border: rgba(255,255,255,0.08);
      --font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", sans-serif;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0; padding: 28px;
      background: radial-gradient(1200px 800px at 20% -10%, rgba(122,162,255,0.12), transparent 50%), var(--bg);
      color: var(--text); font-family: var(--font); line-height: 1.5;
    }}
    .wrap {{ max-width: 1000px; margin: 0 auto; }}
    .header {{ margin-bottom: 28px; }}
    h1 {{ font-size: 1.85rem; margin: 0 0 8px 0; font-weight: 700; }}
    .subtitle {{ color: var(--muted); font-size: 0.95rem; margin: 0; }}
    a {{ color: var(--accent); text-decoration: none; }}
    a:hover {{ text-decoration: underline; }}
    .card {{
      background: linear-gradient(180deg, rgba(255,255,255,0.03), transparent 20%), var(--card);
      border: 1px solid var(--border); border-radius: 14px; padding: 22px; margin-bottom: 22px;
    }}
    .card-title {{ font-size: 1.15rem; margin: 0 0 14px 0; font-weight: 600; color: var(--text); }}
    .table-wrap {{ overflow-x: auto; border-radius: 8px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th, td {{ padding: 11px 14px; border-bottom: 1px solid var(--border); text-align: left; }}
    th {{ color: var(--muted); font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: 0.04em; }}
    td.num, th.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    tbody tr:hover {{ background: rgba(255,255,255,0.04); }}
    .muted {{ color: var(--muted); font-size: 13px; margin-top: 10px; }}
    .anal-table {{ margin-bottom: 28px; }}
    .footer {{ margin-top: 32px; padding-top: 20px; border-top: 1px solid var(--border); color: var(--muted); font-size: 13px; }}
  </style>
</head>
<body>
  <div class="wrap">
    <header class="header">
      <h1>Прайс Mercimed.by</h1>
      <p class="subtitle">Услуги и операции по специальностям. Источник: <a href="{esc(source_url)}" target="_blank" rel="noopener">{esc(source_url)}</a>. Собрано через Apify Puppeteer Scraper (без LLM).</p>
    </header>

    <section class="card anal-table">
      <h2 class="card-title">Анализ по специальностям</h2>
      <div class="table-wrap">
        <table>
          <thead><tr><th>Специальность</th><th class="num">Всего</th><th class="num">С ценой</th><th class="num">Мин, руб</th><th class="num">Макс, руб</th><th class="num">Средняя, руб</th></tr></thead>
          <tbody>{anal_tr}</tbody>
        </table>
      </div>
    </section>

    {"".join(rows_html)}

    <footer class="footer">
      Данные взяты с сайта mercimed.by в ознакомительных целях. Актуальность уточняйте на сайте или по телефону клиники.
    </footer>
  </div>
</body>
</html>"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(doc, encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Скрапинг прайса mercimed.by через Apify → HTML-отчёт (без LLM)")
    parser.add_argument(
        "--url",
        default="https://mercimed.by/uslugi/khirurgiya/",
        help="Стартовый URL или несколько через запятую (напр. .../khirurgiya/, .../ortopediya/). Обход подстраниц — только при одном URL.",
    )
    parser.add_argument("-o", "--output", type=Path, default=Path("output/mercimed_report.html"), help="Путь к HTML-отчёту")
    parser.add_argument("--wait", type=int, default=500, help="Макс. ожидание завершения run, сек")
    parser.add_argument("--no-crawl", action="store_true", help="Не обходить подстраницы, только перечисленные --url")
    parser.add_argument("--max-pages", type=int, default=40, help="Макс. страниц при обходе подстраниц")
    args = parser.parse_args()

    token = (os.environ.get("APIFY_TOKEN") or "").strip()
    if not token:
        print("Не задан APIFY_TOKEN. Скопируй .env.example → .env и укажи токен.", file=sys.stderr)
        return 1

    base = (os.environ.get("APIFY_BASE_URL") or BASE_URL).strip()
    start_urls = [u.strip() for u in args.url.split(",") if u.strip()]
    crawl = not args.no_crawl and len(start_urls) <= 1
    print("Запуск Apify Puppeteer Scraper:", start_urls[:3], "…" if len(start_urls) > 3 else "", "обход подстраниц:", crawl, file=sys.stderr)
    try:
        items = run_puppeteer_scraper(
            start_urls,
            token,
            base_url=base,
            wait_timeout=args.wait,
            crawl_subpages=crawl,
            max_pages=args.max_pages,
        )
    except Exception as e:
        print(f"Ошибка: {e}", file=sys.stderr)
        return 1

    if not items:
        print("Нет записей в выводе Apify — пишу отчёт с сообщением. Сохраняю сырой ответ в .raw.json для отладки.", file=sys.stderr)
        raw_path = args.output.with_suffix("").with_suffix(".raw.json")
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(json.dumps({"count": 0, "sample": "run ok, dataset empty or not fetched"}, ensure_ascii=False, indent=2), encoding="utf-8")
        by_spec = {}
        analysis = []
    else:
        by_spec = by_specialty_from_apify_items(items)
        analysis = analysis_rows(by_spec)
        if not by_spec or all(not v for v in by_spec.values()):
            raw_path = args.output.with_suffix("").with_suffix(".raw.json")
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_text(json.dumps(items[:5], ensure_ascii=False, indent=2), encoding="utf-8")
            print("Все записи с пустыми items — образец в", raw_path, file=sys.stderr)

    out_path = args.output
    first_url = args.url.split(",")[0].strip() if "," in args.url else args.url.strip()
    source_label = first_url
    write_html_report(out_path, by_spec, analysis, source_label)
    print("Отчёт записан:", out_path, file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
