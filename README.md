# Instagram: Apify‑скрапер + LLM‑анализ

Скрапит несколько Instagram‑профилей через [apify/instagram-scraper](https://apify.com/apify/instagram-scraper), кэширует в SQLite, строит сводки и при желании запускает LLM‑анализ (OpenAI‑совместимый API).

---

## Как пользоваться

### 1. Установка

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Настройка

Создай `.env` из примера и заполни обязательные поля:

```bash
cp .env.example .env
```

В `.env` обязательно:

- **`APIFY_TOKEN`** — токен с [Apify](https://console.apify.com/account/integrations).

Для LLM‑анализа (опционально):

- **`LLM_API_KEY`** или **`OPENAI_API_KEY`**
- **`LLM_MODEL`** (например `gpt-4o-mini`)
- при необходимости **`LLM_BASE_URL`** (по умолчанию `https://api.openai.com/v1`)

Период можно задать в `.env`: `START_DATE`, `END_DATE` или `PERIOD=this-month|last-month|last-30d`.

### 3. Цели скрапинга

Профили берутся из **`targets.txt`** (по одной ссылке на строку) или из **`targets.csv`** (колонки `profile_url`, `label`, `start`, `end`). Примеры уже лежат в репозитории.

### 4. Запуск

**Только скрапинг** (без LLM, только таблицы и отчёт):

```bash
python ig_apify_scrape.py
```

**Скрапинг + LLM‑анализ** (рекомендации, темы, сравнение с baseline):

```bash
python ig_apify_scrape.py --analysis llm
```

**Результат в папку `output/`:**

- `items_<start>_to_<end>.csv` — все посты/рилсы
- `summary_<start>_to_<end>.csv` — сводка по аккаунтам
- `report_<start>_to_<end>.html` — отчёт в браузере
- при `--analysis llm`: `llm_insights_*.json`, benchmark CSV и т.п.

### 5. Публикация отчёта (например, в GitHub Pages)

Сгенерировать отчёт в `docs/` и обновить индекс:

```bash
python ig_apify_scrape.py --publish-dir docs --analysis llm
```

На GitHub: **Settings → Pages → Source**: branch `master`, папка `/docs`.  
Для автообновления: скопировать `workflow_template_scrape_and_publish.yml` в `.github/workflows/scrape_and_publish.yml` и добавить секреты `APIFY_TOKEN` и при необходимости `OPENAI_API_KEY`.

---

## Полезные флаги

| Флаг | Значение |
|------|----------|
| `--analysis llm` | Включить LLM‑анализ (нужен ключ в `.env`) |
| `--analysis none` | Только скрапинг, без LLM |
| `--refresh auto` | Перескрапывать только при появлении новых постов (по умолчанию) |
| `--refresh always` | Всегда перескрапывать |
| `--refresh never` | Использовать только кэш |
| `--start` / `--end` | Период в формате `YYYY-MM-DD` |
| `--period this-month` | Удобные периоды: `this-month`, `last-month`, `last-30d` |
| `--publish-dir docs` | Складывать отчёт и CSV/JSON в указанную папку |

---

## Кэш

Состояние хранится в SQLite (`DB_PATH`, по умолчанию `cache/instagram_apify.sqlite`). При `--refresh auto` повторный скрап по профилю не делается, если новых постов нет.

---

## Схема Apify

Input actor: https://apify.com/apify/instagram-scraper/input-schema

---

## Прайс Mercimed (услуги и цены по специальностям)

Отдельный скрипт **`mercimed_prices_scraper.py`** собирает названия операций/услуг и их стоимость с mercimed.by по разделам `/uslugi/...`, строит таблицу по специальностям и анализ (количество, мин/макс/средняя/медиана цены).

**Зависимости:** `requests`, `beautifulsoup4` (уже в `requirements.txt`).

**Запуск по URL** (обход в глубину по ссылкам `/uslugi/...`):

```bash
python mercimed_prices_scraper.py --url "https://mercimed.by/uslugi/khirurgiya/" -o output
```

**Результат в `output/`:**
- `mercimed_prices_by_specialty.csv` — таблица: специальность, название услуги, цена (руб)
- `mercimed_analysis_by_specialty.csv` — по каждой специальности: число услуг, с ценой, мин/макс/средняя/медиана цены

**Если сайт отдаёт «Секундочку…» и контент подгружается по JS**, можно сохранить страницу вручную и разобрать из файла:

1. Открой в браузере, например: https://mercimed.by/uslugi/khirurgiya/
2. «Сохранить как» → «Веб-страница, полностью»
3. Запуск:

```bash
python mercimed_prices_scraper.py --from-html путь/к/khirurgiya.html --specialty khirurgiya -o output
```

Имя специальности можно не указывать — тогда берётся из имени файла (без расширения).

**Скрапинг через Apify (Puppeteer, без LLM):**

```bash
python mercimed_apify_scraper.py --url "https://mercimed.by/uslugi/khirurgiya/" -o output/mercimed_report.html
```

Результат — один HTML-файл с таблицей «Анализ по специальностям» и таблицами по каждой специальности. Нужен `APIFY_TOKEN` в `.env`.

- **Обход всех подстраниц** в разделе (хирургия и её подразделы): не указывать `--no-crawl`, увеличить ожидание, например  
  `--wait 600 --max-pages 30`.
- **Несколько URL за один запуск** (без перехода по ссылкам):  
  `--url "https://mercimed.by/uslugi/khirurgiya/,https://mercimed.by/uslugi/napravleniya/ortopediya/" --no-crawl`.
- Главная страница «Хирургия» — по сути меню; цены обычно на подстраницах (гинекологические операции, ортопедия и т.п.), поэтому имеет смысл либо обход с `--no-crawl` выключенным, либо явный список подстраниц через `--url url1,url2,... --no-crawl`.
