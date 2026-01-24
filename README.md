# Instagram: Apify‑скрапер (мульти‑профили + период + кэш + сводная таблица)

Этот проект **использует только Apify** (`apify/instagram-scraper`) и умеет:

- скрапить **несколько Instagram профилей** из `targets.csv`
- скрапить **за нужный период** (`start`..`end`, end-exclusive)
- **не скрапить повторно** то, что уже было собрано за этот период (кэш в SQLite)
- собрать **общую таблицу** со всеми постами + **сводку/анализ по аккаунтам**

## Что нужно

- Apify аккаунт и `APIFY_TOKEN`
- Заполнить `.env` (см. `.env.example`)

## Файл целей `targets.csv`

Формат:

- `profile_url` — ссылка на профиль
- `label` — как назвать аккаунт в отчёте
- `start` — YYYY-MM-DD (в UTC)
- `end` — YYYY-MM-DD (end-exclusive, в UTC)

Пример уже лежит в `targets.csv`.

## Запуск

1) Установить зависимости:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2) Создать `.env`:

```bash
cp .env.example .env
```

Заполнить `APIFY_TOKEN`.

3) Запустить скрап:

```bash
python3 ig_apify_scrape.py
```

После выполнения появятся CSV:
- `output/items_<start>_to_<end>.csv` — все посты/рилсы всех аккаунтов за период
- `output/summary_<start>_to_<end>.csv` — сводка/анализ по аккаунтам за период
И HTML отчёт (локальная веб‑страница):
- `output/report_<start>_to_<end>.html` — удобочитаемый отчёт с таблицами и графиками + ссылками на CSV

## Публикация отчёта “через интернет” (GitHub Pages)

1) Сгенерировать и положить отчёт в `docs/`:

```bash
./.venv/bin/python ig_apify_scrape.py --publish-dir docs --update
```

2) На GitHub включить Pages: Settings → Pages → **Deploy from a branch** → Branch: `master`, Folder: `/docs`.
После этого отчёт будет доступен по ссылке GitHub Pages (вкладка Pages покажет URL).

3) Автообновление (опционально): добавь GitHub Action workflow.

- Скопируй файл `workflow_template_scrape_and_publish.yml` в `.github/workflows/scrape_and_publish.yml` (в репозитории).
- Добавь секрет `APIFY_TOKEN` в Settings → Secrets and variables → Actions → New repository secret.
- Дальше можно запускать workflow вручную (Actions) или ждать расписания.

## Кэш / “уже скрапил”

Состояние хранится в SQLite (`DB_PATH`, по умолчанию `cache/instagram_apify.sqlite`).

- По умолчанию, если профиль уже помечен как “скрапнутый” для периода (`start`..`end`), он **пропускается** (экономит деньги в Apify).
- Если нужно перескрапить/обновить — запусти с `--update`.

Схема input actor: `https://apify.com/apify/instagram-scraper/input-schema`
