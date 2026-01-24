# Instagram: Apify‑скрапер (мульти‑профили + период + кэш + сводная таблица)

Этот проект **использует только Apify** ([`apify/instagram-scraper`](https://apify.com/apify/instagram-scraper)) и умеет:

- скрапить **несколько Instagram профилей** из `targets.txt` (или `targets.csv`)
- скрапить **за нужный период** (`start`..`end`, end-exclusive)
- **проверять, появились ли новые публикации** и только тогда обновлять (режим `--refresh auto`)
- обновлять метрики (лайки/комменты/просмотры) при перескрапе
- делать **анализ тем по тексту/хэштегам**: где больше лайков/просмотров
- собрать **общую таблицу** со всеми постами + **сводку/анализ по аккаунтам**

## Что нужно

- Apify аккаунт и `APIFY_TOKEN`
- Заполнить `.env` (см. `.env.example`)

## Файл целей (проще всего) — `targets.txt`

- По одной ссылке на профиль в строке.
- Период задаётся один раз через `.env` (или флаги запуска).

Пример уже лежит в `targets.txt`.

## Альтернатива — `targets.csv`

Если нужно хранить разные периоды/лейблы в одном файле: `profile_url,label,start,end`.

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

Период можно задать в `.env`:
- `START_DATE` / `END_DATE` (end-exclusive)
- или `PERIOD=this-month|last-month|last-30d`

3) Запустить скрап:

```bash
python3 ig_apify_scrape.py
```

После выполнения появятся CSV:
- `output/items_<start>_to_<end>.csv` — все посты/рилсы всех аккаунтов за период
- `output/summary_<start>_to_<end>.csv` — сводка/анализ по аккаунтам за период
- `output/topics_<start>_to_<end>.csv` — анализ тем (слова/хэштеги) и их лайки/просмотры
И HTML отчёт (локальная веб‑страница):
- `output/report_<start>_to_<end>.html` — удобочитаемый отчёт с таблицами и графиками + ссылками на CSV

## Публикация отчёта “через интернет” (GitHub Pages)

1) Сгенерировать и положить отчёт в `docs/`:

```bash
./.venv/bin/python ig_apify_scrape.py --publish-dir docs
```

2) На GitHub включить Pages: Settings → Pages → **Deploy from a branch** → Branch: `master`, Folder: `/docs`.
После этого отчёт будет доступен по ссылке GitHub Pages (вкладка Pages покажет URL).

3) Автообновление (опционально): добавь GitHub Action workflow.

- Скопируй файл `workflow_template_scrape_and_publish.yml` в `.github/workflows/scrape_and_publish.yml` (в репозитории).
- Добавь секрет `APIFY_TOKEN` в Settings → Secrets and variables → Actions → New repository secret.
- Дальше можно запускать workflow вручную (Actions) или ждать расписания.

## Кэш / “уже скрапил”

Состояние хранится в SQLite (`DB_PATH`, по умолчанию `cache/instagram_apify.sqlite`).

- По умолчанию включён режим `--refresh auto`: если **новых постов нет**, профиль пропускается (экономит деньги в Apify).
- Если новые посты появились — профиль перескрапится “за весь период” и метрики обновятся (upsert в SQLite).
- Принудительно:
  - `--refresh always` (всегда перескрапить)
  - `--refresh never` (никогда не перескрапить, только один раз)

Схема input actor: `https://apify.com/apify/instagram-scraper/input-schema`
