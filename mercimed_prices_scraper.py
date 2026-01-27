#!/usr/bin/env python3
"""
Скрапер прайса услуг с mercimed.by: названия операций/услуг и их стоимость.
Обходит разделы по ссылкам из /uslugi/..., собирает таблицу по специальностям, строит анализ.
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://mercimed.by"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
REQUEST_TIMEOUT = 30


def _session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    s.headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
    s.headers["Accept-Language"] = "ru-RU,ru;q=0.9,en;q=0.8"
    s.headers["Referer"] = f"{BASE_URL}/"
    return s


def _same_domain(url: str) -> bool:
    return urlparse(url).netloc == urlparse(BASE_URL).netloc


def _is_uslugi_link(href: str) -> bool:
    if not href or href.startswith("#"):
        return False
    path = urlparse(href).path.strip("/")
    return path.startswith("uslugi/") or path == "uslugi"


def _normalize_price(text: str) -> float | None:
    """Из строки вроде '150.50 руб' или '1 200 BYN' извлечь число."""
    if not text or not isinstance(text, str):
        return None
    # убрать тонкие пробелы и заменить запятую на точку
    s = text.replace("\u202f", " ").replace("\xa0", " ").replace(",", ".")
    # число: целое или с точкой, возможно с пробелами между разрядами
    m = re.search(r"(\d[\d\s.]*\d|\d+)", s)
    if not m:
        return None
    val = m.group(1).replace(" ", "").strip()
    try:
        return float(val)
    except ValueError:
        return None


def _extract_from_table(soup: BeautifulSoup) -> list[tuple[str, float | None]]:
    """Попытка извлечь (название, цена) из таблицы."""
    out: list[tuple[str, float | None]] = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        for tr in rows:
            cells = tr.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            texts = [c.get_text(separator=" ", strip=True) for c in cells]
            # ищем ячейку с ценой (руб, byn, бел.руб, число)
            name_cands = []
            price_val: float | None = None
            for t in texts:
                if re.search(r"(руб|byn|бел\.?руб|р\.)", t, re.I):
                    price_val = _normalize_price(t)
                elif re.search(r"^\d[\d\s.,]*$", t):
                    if price_val is None:
                        price_val = _normalize_price(t)
                else:
                    name_cands.append(t)
            name = " ".join(name_cands).strip() if name_cands else texts[0]
            if name and (price_val is not None or name_cands):
                out.append((name, price_val))
    return out


def _extract_from_lists_and_divs(soup: BeautifulSoup) -> list[tuple[str, float | None]]:
    """Поиск блоков вроде 'Название — 100 руб' или списков с ценами."""
    out: list[tuple[str, float | None]] = []
    # типичные классы для прайсов
    for block in soup.find_all(class_=re.compile(r"price|cost|стоимость|usluga|service|item", re.I)):
        text = block.get_text(separator=" ", strip=True)
        if not text or len(text) > 2000:
            continue
        # разбить по числам/руб — считать последнее число в строке ценой
        parts = re.split(r"(\d[\d\s.,]*(?:\s*(?:руб|byn|р\.|бел)))", text, maxsplit=1, flags=re.I)
        if len(parts) >= 2:
            name = parts[0].strip().strip("—:-")
            price_val = _normalize_price(parts[1]) if len(parts) > 1 else None
            if name and len(name) > 2:
                out.append((name, price_val))
        else:
            price_val = _normalize_price(text)
            name = re.sub(r"\d[\d\s.,]*(?:руб|byn|р\.)?", "", text, flags=re.I).strip().strip("—:-")
            if name and len(name) > 2:
                out.append((name, price_val))
    return out


def _extract_by_price_pattern(soup: BeautifulSoup) -> list[tuple[str, float | None]]:
    """Ищем любой текст с числом и 'руб' рядом — считать это цена, остальное название."""
    out: list[tuple[str, float | None]] = []
    for elem in soup.find_all(string=re.compile(r"\d[\d\s.,]*\s*(?:руб|byn|р\.|бел)", re.I)):
        parent = elem.parent
        if not parent:
            continue
        text = parent.get_text(separator=" ", strip=True) if parent else str(elem)
        m = re.search(r"(.+?)\s*(\d[\d\s.,]*)\s*(руб|byn|р\.|бел\.?руб)?", text, re.I)
        if m:
            name = m.group(1).strip().strip("—:-")
            price_val = _normalize_price(m.group(2))
            if name and len(name) > 1:
                out.append((name, price_val))
    return out


def _extract_any_price_like(soup: BeautifulSoup) -> list[tuple[str, float | None]]:
    """Любые блоки, где есть число 50–999999 (похоже на цену) и отдельный текст — название."""
    out: list[tuple[str, float | None]] = []
    for tag in soup.find_all(["div", "li", "tr", "a", "span"], class_=True):
        text = tag.get_text(separator=" ", strip=True)
        if len(text) < 10 or len(text) > 600:
            continue
        m = re.search(r"^(.+?)\s+(\d[\d\s]{2,})\s*(?:руб|byn|р\.|бел\.?руб)?\s*$", text, re.I | re.DOTALL)
        if m:
            name = m.group(1).strip().strip("—:-•")
            price_val = _normalize_price(m.group(2))
            if price_val and 50 <= price_val <= 999999 and name and len(name) > 3:
                out.append((name, price_val))
    return out


def extract_services_from_page(html: str, page_url: str) -> list[tuple[str, float | None]]:
    """Из HTML страницы извлечь пары (название услуги, цена в руб)."""
    soup = BeautifulSoup(html, "html.parser")
    seen: set[str] = set()
    result: list[tuple[str, float | None]] = []

    for extractor in (
        _extract_from_table,
        _extract_from_lists_and_divs,
        _extract_by_price_pattern,
        _extract_any_price_like,
    ):
        for name, price in extractor(soup):
            key = (name[:100], price)
            if key in seen:
                continue
            seen.add(key)
            result.append((name, price))
    return result


def find_uslugi_links(html: str, current_url: str) -> list[str]:
    """Найти все ссылки на разделы /uslugi/... с той же страницы."""
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        full = urljoin(current_url, href)
        if _same_domain(full) and _is_uslugi_link(href):
            path = urlparse(full).path.rstrip("/")
            if path and path != "/uslugi":
                links.append(full)
    return list(dict.fromkeys(links))


def specialty_from_url(url: str) -> str:
    """Короткое имя специальности из URL: uslugi/khirurgiya/ -> khirurgiya."""
    path = urlparse(url).path.strip("/")
    if not path:
        return "main"
    parts = path.split("/")
    if parts[0] == "uslugi" and len(parts) > 1:
        return parts[-1] or parts[1] if len(parts) > 1 else "main"
    return path.replace("/", "_") or "main"


def crawl_prices(
    start_url: str,
    session: requests.Session,
    max_pages: int = 50,
) -> dict[str, list[tuple[str, float | None]]]:
    """
    Обойти start_url и все обнаруженные /uslugi/ ссылки, собрать по каждой
    список (название, цена). Ключ — specialty из URL.
    """
    to_visit = [start_url]
    visited: set[str] = set()
    by_specialty: dict[str, list[tuple[str, float | None]]] = defaultdict(list)

    while to_visit and len(visited) < max_pages:
        url = to_visit.pop(0)
        if url in visited:
            continue
        visited.add(url)
        try:
            r = session.get(url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            r.encoding = r.encoding or "utf-8"
            html = r.text
            # mercimed.by сначала отдаёт «Секундочку…» и через 5 сек перезагружает; при коротком ответе пробуем ещё раз
            if len(html) < 8000 and ("Секундочку" in html or "spinner" in html or "setTimeout" in html):
                time.sleep(6)
                r2 = session.get(url, timeout=REQUEST_TIMEOUT)
                r2.raise_for_status()
                r2.encoding = r2.encoding or "utf-8"
                html = r2.text
        except Exception as e:
            print(f"  [skip] {url}: {e}", file=sys.stderr)
            continue

        spec = specialty_from_url(url)
        items = extract_services_from_page(html, url)
        if items:
            by_specialty[spec].extend(items)
        # добавить новые ссылки только из /uslugi/
        for link in find_uslugi_links(html, url):
            if link not in visited and link not in to_visit:
                to_visit.append(link)
    return dict(by_specialty)


def analyze_by_specialty(
    by_specialty: dict[str, list[tuple[str, float | None]]]
) -> list[dict[str, str | int | float]]:
    """Анализ по специальностям: количество, мин/макс/средняя/медиана цены."""
    rows: list[dict[str, str | int | float]] = []
    for spec, items in sorted(by_specialty.items()):
        prices = [p for _, p in items if p is not None and p > 0]
        count = len(items)
        with_price = len(prices)
        row: dict[str, str | int | float] = {
            "specialty": spec,
            "procedures_count": count,
            "with_price_count": with_price,
        }
        if prices:
            row["price_min_rub"] = round(min(prices), 2)
            row["price_max_rub"] = round(max(prices), 2)
            row["price_avg_rub"] = round(sum(prices) / len(prices), 2)
            sp = sorted(prices)
            mid = len(sp) // 2
            row["price_median_rub"] = round((sp[mid] + sp[-1 - mid]) / 2, 2)
        else:
            row["price_min_rub"] = ""
            row["price_max_rub"] = ""
            row["price_avg_rub"] = ""
            row["price_median_rub"] = ""
        rows.append(row)
    return rows


def write_table_csv(path: Path, by_specialty: dict[str, list[tuple[str, float | None]]]) -> None:
    """Таблица: specialty, procedure_name, price_rub."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["specialty", "procedure_name", "price_rub"])
        for spec in sorted(by_specialty.keys()):
            for name, price in by_specialty[spec]:
                w.writerow([spec, name, price if price is not None else ""])


def write_analysis_csv(path: Path, rows: list[dict[str, str | int | float]]) -> None:
    """Таблица анализа по специальностям."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Скрапинг прайса услуг mercimed.by по разделам /uslugi/..., таблица по специальностям и анализ."
    )
    parser.add_argument(
        "--url",
        default="https://mercimed.by/uslugi/khirurgiya/",
        help="Стартовый URL (в глубину от него идут ссылки /uslugi/...)",
    )
    parser.add_argument(
        "--from-html",
        type=Path,
        default=None,
        metavar="FILE",
        help="Взять одну страницу из локального HTML-файла (без загрузки по сети). Имя специальности — из имени файла или --specialty.",
    )
    parser.add_argument("--specialty", default=None, help="Имя специальности при --from-html (по умолчанию из имени файла).")
    parser.add_argument("--max-pages", type=int, default=50, help="Максимум страниц для обхода")
    parser.add_argument("-o", "--output-dir", type=Path, default=Path("output"), help="Папка для CSV")
    args = parser.parse_args()

    if args.from_html:
        path = Path(args.from_html)
        if not path.exists():
            print(f"Файл не найден: {path}", file=sys.stderr)
            return 1
        spec = args.specialty or path.stem
        html = path.read_text(encoding="utf-8", errors="replace")
        items = extract_services_from_page(html, str(path))
        by_specialty = {spec: items}
        print("Чтение из файла:", path, "→ специальность:", spec, file=sys.stderr)
    else:
        session = _session()
        print("Скрапинг:", args.url, file=sys.stderr)
        by_specialty = crawl_prices(args.url, session, max_pages=args.max_pages)

    out_dir = args.output_dir
    table_path = out_dir / "mercimed_prices_by_specialty.csv"
    analysis_path = out_dir / "mercimed_analysis_by_specialty.csv"

    write_table_csv(table_path, by_specialty)
    print("Таблица:", table_path, file=sys.stderr)

    analysis_rows = analyze_by_specialty(by_specialty)
    write_analysis_csv(analysis_path, analysis_rows)
    print("Анализ:", analysis_path, file=sys.stderr)

    # краткий вывод в консоль
    print("\n--- Анализ по специальностям ---")
    if analysis_rows:
        print(f"{'Специальность':<25} {'услуг':>8} {'с ценой':>8} {'мин':>10} {'макс':>10} {'средняя':>10}")
        print("-" * 75)
        for r in analysis_rows:
            spec = str(r.get("specialty", ""))[:24]
            cnt = r.get("procedures_count", 0)
            wprice = r.get("with_price_count", 0)
            mn = r.get("price_min_rub", "")
            mx = r.get("price_max_rub", "")
            av = r.get("price_avg_rub", "")
            print(f"{spec:<25} {cnt:>8} {wprice:>8} {mn!s:>10} {mx!s:>10} {av!s:>10}")
    else:
        print("Нет данных. Возможно, структура страниц не подошла под текущие селекторы — пришлите пример HTML нужного блока.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
