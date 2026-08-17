# CLAUDE.md

## Цель сейчас

Довести `lt_app` до состояния, когда Streamlit-приложение реально запускается в Docker и проходит один тестовый прогон. Всё остальное — вторично.

Единственный активный документ статуса — `HANDOFF.md` в корне репозитория. Читай его в начале каждой сессии.

## Абсолютный запрет: архив

```text
docs/archive/
```

Никогда не читай, не открывай, не listай и не упоминай содержимое `docs/archive/`. Там лежат устаревшие промпты, диалоги и черновики прошлых итераций. Они не отражают текущее состояние кода и только тратят контекст. Если тебе кажется, что там есть нужная информация — она уже либо перенесена в `HANDOFF.md`, либо не нужна.

## Жёсткие рамки работы

- Работай в текущей git-ветке. Не создавай, не переключай ветки.
- Не делай `git add`, commit, push, rebase, merge, force push без явной команды.
- Не запускай `Explore`, `Plan`, subagents, background agents, рекурсивный аудит кодовой базы, широкие поисковые задачи типа «изучи всё» или «проверь архитектуру».
- Не читай и не печатай в чат: `artifacts/`, `data/`, `.env`, секреты, сертификаты, большие CSV/JSON с реальными данными, `.venv/`, `node_modules/`, `.git/`.
- Не печатай содержимое файлов целиком, если explicitly не попросили. Показывай только diff или конкретные строки, максимум ~40 строк за раз.
- Одна задача — одна короткая итерация: план (≤5 строк) → изменение → команда проверки → короткий отчёт (изменённые файлы, exit code, ошибка если есть).
- Не расширяй scope, не предлагай альтернативную архитектуру, не переписывай уже работающий код без прямого запроса.
- Если контекст начинает разрастаться — остановись и явно скажи об этом, вместо того чтобы продолжать читать файлы.

## Текущая архитектура (после Step 5 — актуально)

```text
Streamlit UI (app.py)
    │
    ▼
SessionWorker (worker.py)
    ├── KafkaEventGenerator
    ├── OpenSearchIndexMonitor
    ├── KafkaJsonArtifactCollector
    ├── EtlReportStatisticsBuilder
    ├── EtlStatisticsProjectionBuilder
    ├── EtlStageSizeAggregateBuilder
    └── EtlThroughputAggregateBuilder
```

Единый источник конфигурации — `lt_app/src/runtime_settings.py`:

- `FIELD_REGISTRY` — декларативный список всех runtime-настроек (canonical `section.field`, тип, default, env var, UI widget).
- Все Settings-классы модулей — обычные `pydantic.BaseModel`. **Не** `pydantic_settings.BaseSettings`. Модули не читают `os.environ`/`.env` сами.
- Только `runtime_settings.py` вызывает `os.environ`/`os.getenv` — через `load_env_settings()`.
- `effective_settings` — единственный mutable словарь настроек до старта теста. Собирается как `defaults → ENV → YAML import → UI edit`, где побеждает последняя по времени операция на конкретном field path (не иерархия источников).
- `run_settings = freeze_settings(effective_settings)` — immutable snapshot, передаётся в `SessionWorker` при старте. После старта UI-правки не влияют на текущий запуск.
- ENV var naming: `LT_APP__<SECTION>__<FIELD>`.
- Пароли/credentials хранятся как обычный `str` в registry и YAML export — это внутренняя developer-тулза, экспорт всегда полный и открытый.

Если видишь код, ссылающийся на `KAFKA_EVENT_GENERATOR_`, `OPENSEARCH_INDEX_MONITOR_` prefix или `BaseSettings`/`SettingsConfigDict` в runtime-модулях — это остаток старой схемы и должен быть удалён в рамках текущей задачи, не восстановлен.

## Что можно менять без вопроса

- `lt_app/app.py` — интеграция с `runtime_settings.py`, UI построение из `FIELD_REGISTRY`.
- Мелкие правки внутри уже перечисленных runtime-модулей, если они не меняют Kafka message contract или ETL-аналитическую логику.

## Что требует явного разрешения

- Любые изменения бизнес-логики генерации/обработки ETL-событий.
- Изменение форматов Kafka-сообщений.
- Docker/инфраструктурные файлы.
- Массовое перемещение или удаление файлов.
- Добавление новых зависимостей.

## Проверки после любого изменения

```bash
python3 -m py_compile lt_app/app.py lt_app/src/*.py
grep -R "BaseSettings\|SettingsConfigDict\|os.getenv\|os.environ" -n lt_app/app.py lt_app/src
```

`grep` должен находить `os.environ`/`os.getenv` только внутри `lt_app/src/runtime_settings.py`. Любое другое совпадение — регрессия, которую нужно исправить, а не игнорировать.
