# lt_app

`lt_app` — инструмент нагрузочного тестирования внешнего ETL-сервиса. Он воспроизводит события из CSV-дампа в тестовый Kafka-topic, ограничивает скорость публикации по lag consumer group тестируемого ETL, собирает диагностические артефакты и строит отчёт по результатам тестовой сессии.

Проект предназначен для управляемых тестовых контуров. Он не является ETL-сервисом и не должен изменять логику тестируемого ETL.

## Цель

Инструмент должен позволять провести воспроизводимый нагрузочный заход:

1. Выбрать CSV с событиями и параметры теста.
2. Запустить публикацию в Kafka с ограничением по lag тестируемой consumer group.
3. Параллельно собирать document count выбранных OpenSearch-индексов.
4. Параллельно сохранять диагностические JSON-сообщения из Kafka debug/error topic.
5. По завершении построить табличную статистику успешных и неуспешных обработок.
6. Построить регрессионные прогнозы времени и памяти по размеру вложений.
7. Сохранить артефакты и итоговое описание тестовой сессии.

UI планируется на Streamlit. Streamlit отвечает за редактирование параметров, Start/Stop, отображение состояния, загрузку и выгрузку YAML-конфигурации и скачивание артефактов. Бизнес-логика Kafka, OpenSearch и аналитики не должна находиться в Streamlit-скрипте.

## Поток тестовой сессии

```text
CSV dump
  │
  ▼
Kafka event generator ─────────────► test Kafka topic ─────► tested ETL service
  │                                            │
  │ lag of tested ETL consumer group            │ debug/error topic
  ▼                                            ▼
OpenSearch index monitor                  Kafka JSON artifact collector
  │                                            │
  ▼                                            ▼
index_counts.csv                         artifacts/err_events/*.json
                                                 │
                                                 ▼
                                      ETL report statistics builder
                                                 │
                                                 ▼
                                  stats_done.csv / stats_skipped.csv
                                                 │
                                                 ▼
                                    ETL statistics projection builder
                                                 │
                                                 ▼
                       coefficients.csv / projection_by_stage.csv /
                                      projection_total.csv
```

## Модули

| Модуль | Назначение | Режим работы |
|---|---|---|
| `kafka_event_generator.py` | Читает CSV-дамп и публикует события в target topic | Долгоживущая задача до лимита сообщений или Stop |
| `opensearch_index_monitor.py` | Снимает document count заданных OpenSearch-индексов | Параллельный monitor до Stop |
| `kafka_json_artifact_collector.py` | Сохраняет JSON из Kafka debug/error topic | Параллельный consumer до Stop или лимита |
| `etl_report_statistics.py` | Разбирает JSON-артефакты в DONE/skipped/read-failures CSV | Конечная аналитическая задача |
| `etl_statistics_projection.py` | Строит регрессии и прогнозы по `stats_done.csv` | Последняя конечная задача |
| `worker.py` | Запускает задачи сессии, передаёт progress/events, обрабатывает Stop | Worker-процесс сессии |
| `app.py` | Streamlit UI и управление тестовой сессией | Основной процесс UI |

## Архитектурные принципы

### Оркестратор и workers

Streamlit не должен напрямую владеть Kafka/OpenSearch-клиентами, event loop или долгими задачами. Он создаёт worker-процесс тестовой сессии и получает от него только компактные progress-события и итоговые результаты.

Один worker-процесс соответствует одной тестовой сессии и содержит общий lifecycle её задач:

- event generator;
- OpenSearch monitor;
- Kafka artifact collector;
- post-processing и построение итоговых артефактов.

Внутри worker-а независимые задачи могут выполняться через `asyncio`. Несколько независимых Kafka-конфигураций в будущем должны поддерживаться несколькими worker-процессами, а не смешиванием клиентов в Streamlit UI.

### Конфигурация

Конфигурация разделена на два слоя:

- **YAML session config**: экспортируемые и редактируемые в UI не-секретные параметры теста — topic, consumer group, CSV path, лимиты, интервалы, индексы, режимы и описание тестового захода.
- **Environment / локальный `.env`**: секреты и инфраструктурные параметры — пароли, TLS paths, SASL credentials, приватные hosts и другие значения, которые не должны попадать в Git или выгруженный YAML.

Для модулей используются Pydantic v2 и `pydantic-settings`. Настройки отдельных модулей должны быть типизированы и могут иметь собственные env prefixes, но их экземпляры создаёт supervisor. Не каждый модуль должен сам решать, где искать конфигурацию сессии.

Реальный `.env` игнорируется Git. В репозитории хранится только `.env.example` без реальных секретов.

### Артефакты

Все результаты одной сессии должны лежать в отдельном каталоге:

```text
artifacts/<session_id>/
├── session.json
├── index_counts.csv
├── err_events/
├── stats_done.csv
├── stats_skipped.csv
├── stats_read_failures.csv
├── coefficients.csv
├── projection_by_stage.csv
├── projection_total.csv
└── report.md
```

Большие JSON-артефакты, CSV-дампы и результаты тестов не должны записываться в корень репозитория и не должны попадать в Git.

## Важные семантики

### Kafka event generator

- CSV содержит экспортированные Kafka-события с полями `offset`, `timestamp`, `partition`, `key`, `value`.
- Payload из CSV передаётся как исходные bytes. Его нельзя декодировать, модифицировать и пересобирать через `json.dumps`.
- Consumer group для контроля lag должна совпадать с consumer group тестируемого ETL-сервиса.
- Generator приостанавливает публикацию, пока lag не снизится ниже заданного порога.
- `max_messages` означает суммарное число публикаций, а не число строк в CSV.
- При `repeat_source=true` короткий CSV циклически воспроизводится до `max_messages`.

### Kafka diagnostic artifacts

- Debug/error topic может содержать и ошибки, и объёмные отчёты успешной обработки.
- Каждый JSON сохраняется отдельным атомарно созданным файлом.
- Offset коммитится только после успешной записи файла.
- В UI и progress events передаётся только metadata: topic, partition, offset, путь, счётчики. Payload не передаётся.
- Для нового тестового захода consumer group collector-а обычно должна быть уникальной на session id.

### OpenSearch monitor

- Каждую итерацию фиксируется count каждого заданного индекса.
- Ошибка одного индекса записывается отдельной CSV-строкой и не останавливает мониторинг остальных индексов.
- В закрытом тестовом контуре допускается `verify_certs=false`, если это сознательно задано в конфигурации окружения.

### ETL analytics

- В stage-статистику попадают только задачи и вложения со статусами `DONE` или `SUB_DONE`.
- Не-DONE, отсутствующие метрики и ошибки JSON parsing сохраняются отдельно.
- Регрессии строятся по `(attach_type, stage)`.
- Прогноз вне фактического диапазона размеров должен быть помечен как extrapolation.
- RSS в итоговой модели — максимум peak RSS среди последовательных стадий, а не сумма памяти.

## Состояние MVP

Ближайшая цель — рабочий Streamlit MVP для одной тестовой сессии:

- форма параметров;
- Start/Stop;
- один worker-процесс;
- Kafka producer с lag control;
- OpenSearch count monitor;
- diagnostic artifact collector;
- status/progress/error в UI;
- формирование CSV-артефактов и краткого итогового отчёта.

Параллельные сессии, распределённое выполнение, полноценная авторизация, БД метаданных, сложная визуализация и деплой не являются задачами первого MVP.

## Безопасность и Git

Не коммитить:

- `.env`;
- реальные токены, пароли, ключи и TLS private keys;
- `artifacts/`;
- `err_events/`;
- CSV-дампы реальных сообщений;
- локальные настройки Claude Code: `.claude/settings.local.json`.

Перед commit всегда проверить:

```bash
git status
git diff --check
git diff
```

## Разработка

Точный способ запуска и зависимости будут зафиксированы после добавления `pyproject.toml` или `requirements.txt`. До этого не следует предполагать команды запуска, версии библиотек или доступность внешней Kafka/OpenSearch-инфраструктуры.
