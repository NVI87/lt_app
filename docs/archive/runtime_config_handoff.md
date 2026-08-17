# Handoff: единая runtime-конфигурация lt-app

## Цель

Нужно завершить реализацию единого контракта runtime-настроек для внутренней developer-тулзы `lt-app`.

Для **каждого** runtime-consumed поля должны существовать три равноправных канала:

1. YAML;
2. ENV с именем `LT_APP__<SECTION>__<FIELD>`;
3. UI.

До старта теста существует только один mutable object:

```python
effective_settings
```

Он содержит актуальное значение каждого поля. Изменения накладываются leaf-by-leaf и во временном порядке: defaults → ENV → YAML import → UI edit; последняя по времени операция для конкретного `section.field` побеждает. Перед запуском создаётся независимый snapshot:

```python
run_settings = deep_copy(effective_settings)
```

Worker и runtime-модули используют только этот snapshot; UI-изменения после старта не должны менять текущий запуск.

## Уже сделано

- Проведён аудит runtime-настроек.
- В реестре зафиксировано **65 canonical fields в 8 секциях**:
  - `run` — 1: `report_interval_minutes`;
  - `generator` — 19;
  - `monitor` — 13;
  - `collector` — 14;
  - `statistics` — 7;
  - `projection` — 7;
  - `stage_size` — 2;
  - `throughput` — 2.
- `run.report_interval_minutes` — обязательный canonical field: default 5, допустимый диапазон 1..60, ENV `LT_APP__RUN__REPORT_INTERVAL_MINUTES`, YAML/UI mapping. Он потребляется `SessionWorker._periodic_snapshot_loop()`.
- `session_id` намеренно НЕ является canonical setting: это автоматически генерируемый runtime identifier, не должен быть в YAML/ENV/UI/`effective_settings`.
- Создан `lt_app/src/runtime_settings.py` с registry, defaults, ENV parsing, leaf merge, YAML parsing/export, validation, snapshot/freeze и masking. Перед продолжением обязательно проверить, что файл действительно записан и компилируется:

```bash
python -m py_compile lt_app/src/runtime_settings.py
```

Не переписывать `runtime_settings.py` целиком и не печатать его в чат. Исправлять только при реальной ошибке компиляции/теста.

## Текущая точка

Прошлая сессия дошла до **Step 5: Convert runtime modules from BaseSettings to BaseModel**. Начинать именно с него.

Текущие актуальные файлы проекта включают:

```text
lt_app/app.py
lt_app/src/worker.py
lt_app/src/kafka_event_generator.py
lt_app/src/kafka_json_artifact_collector.py
lt_app/src/opensearch_index_monitor.py
lt_app/src/etl_report_statistics.py
lt_app/src/etl_statistics_projection.py
lt_app/src/etl_stage_size_aggregates.py
lt_app/src/etl_throughput_aggregates.py
lt_app/src/runtime_settings.py
```

## Step 5 — обязательная работа

Конвертировать все runtime Settings-классы из `pydantic_settings.BaseSettings` в обычные `pydantic.BaseModel`.

Затронуты как минимум:

- `KafkaEventGeneratorSettings`;
- `KafkaArtifactCollectorSettings`;
- `OpenSearchIndexMonitorSettings`;
- `EtlReportStatisticsSettings`;
- `EtlStatisticsProjectionSettings`;
- `EtlStageSizeAggregateSettings`;
- `EtlThroughputAggregateSettings`.

Требования:

- удалить импорты `BaseSettings` и `SettingsConfigDict`;
- удалить `model_config` с `env_file`, `env_prefix`, `extra`;
- runtime-модули не должны читать `os.environ`, `.env`, `os.getenv()` или иметь собственный ENV fallback;
- runtime-модули не должны иметь собственные defaults для user-configurable полей: defaults определены только в `runtime_settings.py`;
- поля credentials (`monitor.password`, `generator.sasl_password`, `collector.sasl_password`) на registry/effective-settings уровне — обычные `str | None`, не `SecretStr`;
- допускается на границе создания внешнего клиента извлечь/обернуть credential так, чтобы пароль не логировался, но export YAML обязан содержать исходное открытое значение;
- сохранить существующие field validators/model validators и бизнес-логику Kafka/OpenSearch/ETL;
- не менять форматы Kafka-сообщений и ETL бизнес-логику.

После конвертации модулей запусти:

```bash
pytest -q
```

Если весь suite слишком долгий или требует недоступной инфраструктуры, сначала запусти unit-тесты затронутых модулей и сообщи точную причину ограничений. Не выдумывать PASS.

## Следующие шаги после Step 5

### Step 6: интеграция app.py

Только после успешной конвертации Settings-классов:

- убрать из `app.py` параллельные `MODULE_DEFAULTS`, `WIDGET_SPECS`, ручные parsers и локальные YAML helpers;
- UI строится итерированием по `FIELD_REGISTRY`/section metadata из `runtime_settings.py`;
- единственный pre-run state — `st.session_state.effective_settings`;
- при инициализации: `default_settings()`, затем один явный ENV patch;
- `Apply environment` накладывает `load_env_settings()` поверх текущего state через `merge_settings()`;
- YAML import парсится как patch, накладывается leaf-level; нераспознанные section/field paths показываются пользователю;
- UI edit обновляет только свой canonical field;
- YAML export делает `dump_yaml_settings(effective_settings)` и всегда полный, включая credentials;
- `run.report_interval_minutes` рендерится через тот же registry-driven UI, без отдельной ветки session_state;
- перед запуском валидировать full `effective_settings`, затем `run_settings = freeze_settings(effective_settings)` и передавать этот snapshot в worker.

### Step 7: worker/snapshot/artifact

- Worker не читает ENV.
- Worker получает settings только из `run_settings`.
- `report_interval_minutes` берётся из `run_settings["run"]`.
- Сохранить exact `run_settings.yaml` в артефактах запуска.
- Изменения UI после старта не влияют на уже созданный snapshot.

### Step 8: examples/docs/tests

- Обновить `settings_template.yaml`, `session_settings_example.yaml`, `.env.example` — покрыть все 65 полей и ENV names из registry.
- Добавить тесты для registry coverage, defaults+ENV+YAML+UI leaf merge, YAML round-trip, invalid YAML rollback, ENV export, snapshot isolation, отсутствия `BaseSettings`/ENV-read в runtime modules, и artifact `run_settings.yaml`.

## Правила работы агента

- Не печатать полные файлы, не повторять код и не раздувать контекст.
- Перед изменением: максимум 5 строк плана.
- После изменения: только список файлов, команды тестов и результаты.
- Код показывать только как targeted diff до 40 строк, если реально требуется решение пользователя.
- Не останавливаться для подтверждения между обычными обратимыми edit/test шагами.
- Не менять scope и не предлагать другую архитектуру.
- Не начинать Step 6 до завершения и проверки Step 5.

## Первый запрос новой сессии

```text
Прочитай этот handoff и продолжай с Step 5. Сначала проверь, что `lt_app/src/runtime_settings.py` существует и проходит `python -m py_compile`. Затем конвертируй все перечисленные runtime Settings-классы с BaseSettings на BaseModel по правилам handoff. Не печатай полные файлы: покажи краткий план, внеси изменения, запусти релевантные тесты и дай компактный отчёт.
```
