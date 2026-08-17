# Задача: единая конфигурация runtime-сессии — UI, ENV и YAML (финальная версия)

## Статус и цель

`lt-app` — внутренняя developer-тулза для запуска и наблюдения за ETL-нагрузочными тестами в тестовой инфраструктуре. Она используется в Docker, через корпоративную сеть/VPN, подключается к Kafka и OpenSearch, генерирует события из CSV, собирает Kafka-артефакты, мониторит индексы и строит отчёты.

Нужно реализовать **единый и полный контракт runtime-настроек**.

Каждая настройка, которая фактически используется любым runtime-компонентом приложения, должна быть доступна тремя способами:

1. Через UI.
2. Через environment variable.
3. Через YAML.

Это императивное требование. Не предлагать иной механизм конфигурации, не ограничивать YAML из соображений production-security и не заменять это требование на env-only/secret-manager подход. Это внутренняя developer-тулза; для удобства тестового запуска полный YAML, включая креды, допустим и обязателен.

---

## Модель precedence — читать внимательно, это временной порядок, а не иерархия

Три источника — это не "выбор одного из трёх" и не жёсткая иерархия приоритетов. Это последовательное наложение патчей на один и тот же объект `effective_settings`, где **каждая следующая по времени операция перекрывает только те field path, которые она явно указывает**.

Типичный порядок операций во времени:

```text
1. defaults           — применяются один раз при инициализации новой сессии
2. Apply environment  — явное действие пользователя/старта, накладывает ENV patch
3. YAML import         — явное действие, накладывает YAML patch (leaf-level merge)
4. UI edit             — обновляет ровно один field path сразу в момент правки
```

Правило: **последняя по времени операция по конкретному field path побеждает**, независимо от того, из какого источника она пришла. Если пользователь после импорта YAML вручную поменял поле в UI — UI-правка победит для этого поля, потому что она произошла позже, а не потому что UI "главнее" YAML. Если после этого пользователь повторно применит ENV через явную кнопку "Apply environment" — ENV снова победит для тех полей, которые в нём заданы. Порядок 1→2→3→4 выше — это ожидаемый типичный сценарий (ENV подкладывается при старте контейнера, YAML — заготовленный конфиг, UI-правки — точечные), а не закон, запрещающий более позднее применение ENV или YAML.

`effective_settings` в любой момент до старта теста должен содержать самую последнюю по времени версию каждого поля, вне зависимости от источника. UI обязан отображать именно это состояние немедленно и без задержки — не "черновик", не отдельный YAML-текстовый буфер, не второй параллельный словарь.

---

## Обязательный результат

До запуска НТ существует один и только один объект:

```python
effective_settings
```

Он содержит **полный набор текущих фактических runtime-настроек**.

Все действия до запуска сессии изменяют только этот объект:

- применение ENV (`Apply environment`);
- импорт YAML;
- ручные изменения в UI;
- восстановление дефолтов (`Reset to defaults`);
- экспорт YAML.

Worker не должен собирать настройки самостоятельно, читать env или иметь неявные fallback-значения. При запуске он получает immutable snapshot:

```python
run_settings = deep_copy(effective_settings)
```

Именно `run_settings` использует весь запуск: generator, collector, OpenSearch monitor, статистика, агрегация и отчёты.

---

## Непереговорные инварианты

### 1. Единственный source of truth

До запуска НТ UI, YAML export и настройки, которые получит worker, должны отражать одно и то же состояние:

```text
UI == effective_settings == YAML export == worker input
```

После старта:

```text
worker input == immutable run_settings
```

Изменение UI не может изменять уже запущенную сессию.

### 2. Независимое смешивание источников

Пользователь должен иметь возможность задать произвольную комбинацию:

- часть полей — в YAML;
- часть полей — через UI;
- часть полей — через environment variables.

Например:

```text
generator.topic                ← YAML
collector.bootstrap_servers    ← ENV
monitor.host                   ← YAML
monitor.username               ← UI
monitor.password               ← UI
monitor.index_names            ← ENV
monitor.output_csv_path        ← UI
```

После этого export обязан содержать **полный** текущий набор значений всех полей.

Нельзя перетирать всю секцию `monitor`, `generator` или `collector`, если изменилось одно вложенное поле. Слияние выполняется только на уровне конкретного canonical field path (leaf-level merge), согласно правилу precedence выше.

### 3. Export всегда полный

Экспорт YAML всегда сериализует **весь** `effective_settings`, а не:
- только импортированные YAML-поля;
- только значения UI;
- только непустые значения;
- шаблон с дефолтами;
- состояние на момент старта приложения.

Если пользователь импортировал YAML, затем поменял поле в UI, экспорт обязан содержать новое значение UI.

Если пользователь задал поле через env, экспорт обязан включать resolved value этого поля **в открытом виде**, включая credentials/пароли — см. правило про `SecretStr` ниже.

YAML export должен быть самодостаточным: его можно импортировать в чистую сессию UI и восстановить все runtime-настройки.

### 4. Нет скрытых runtime-настроек

Запрещены:
- `os.getenv()` внутри runtime-модулей;
- `pydantic_settings.BaseSettings` с `env_prefix`/`env_file` внутри runtime-модулей (`kafka_event_generator.py`, `kafka_json_artifact_collector.py`, `opensearch_index_monitor.py`, `etl_*.py`) — эти классы сами читают `os.environ`/`.env` при инструкции, что является скрытым runtime env-чтением и прямо нарушает это правило;
- скрытые конфигурационные значения в `app.py`, `worker.py`, generator, collector, monitor или report-модулях;
- значения, доступные только через UI, только через ENV или только в YAML;
- поля, используемые runtime-кодом, но отсутствующие в реестре конфигурации;
- `*_env` как отдельные runtime-поля вместо самих значений.

**Обязательное изменение:** классы настроек модулей (`KafkaEventGeneratorSettings`, `OpenSearchIndexMonitorSettings`, `KafkaArtifactCollectorSettings`, `EtlReportStatisticsSettings`, `EtlStatisticsProjectionSettings`) должны стать обычными `pydantic.BaseModel` (или `dataclass` с валидацией), без наследования от `BaseSettings` и без `env_prefix`/`env_file`. Всё ENV-чтение выполняется исключительно в `runtime_settings.py::load_env_settings()`, единственном месте, где явно вызывается `os.environ`/`os.getenv`.

Пример: если OpenSearch monitor использует `username` и `password`, то canonical fields — это:

```yaml
monitor:
  username: "..."
  password: "..."
```

И эти же поля обязаны иметь UI и env mapping. Не хранить в effective settings только имя env-переменной.

### 5. Пароли и credentials хранятся как обычная строка на уровне registry

`effective_settings` хранит `monitor.password` и любые другие credentials как обычный `str`, а не `pydantic.SecretStr`. `SecretStr`-обёртка (если модулю она нужна для клиента, например `OpenSearchIndexMonitor`) создаётся **только внутри runtime-модуля непосредственно перед вызовом клиента**, а не в registry и не в `effective_settings`.

Причина: `yaml.safe_dump`/`str()` на объекте `SecretStr` сериализует его как `**********`, что делает YAML export нечестным и нарушает явное требование не скрывать креды из export для этой internal developer-тулзы. Registry обязан экспортировать пароль в открытом виде.

---

## Границы задачи

### Входит

- `lt_app/app.py`;
- `lt_app/src/worker.py`;
- `lt_app/src/kafka_event_generator.py`;
- `lt_app/src/kafka_json_artifact_collector.py`;
- `lt_app/src/opensearch_index_monitor.py`;
- все `etl_*.py`, если они читают или используют конфигурационные параметры;
- `settings_template.yaml`;
- `session_settings_example.yaml`;
- `.env.example`;
- актуальные и новые тесты.

### Не входит

- изменение бизнес-логики генерации ETL-событий;
- изменение контрактов сообщений Kafka;
- изменение формата выходной статистики, кроме передачи ей полной конфигурации;
- внедрение внешнего secret manager;
- продовая security-политика;
- Kubernetes deployment/refactoring инфраструктуры.

---

## Шаг 1. Полный аудит настроек

До реализации агент обязан:

1. Прочитать полностью каждый runtime-файл из границ задачи.
2. Выписать каждое значение, которое:
   - читается из аргумента;
   - читается из словаря/конфига;
   - имеет дефолт;
   - влияет на подключение;
   - влияет на генерацию;
   - влияет на collector;
   - влияет на мониторинг;
   - влияет на файлы/пути/артефакты;
   - влияет на таймауты, лимиты, TLS, auth, consumer offsets или агрегацию.
3. Составить исчерпывающий реестр полей.
4. Для каждого поля указать:
   - canonical path, например `monitor.request_timeout_sec`;
   - Python type;
   - default;
   - required/optional;
   - YAML path;
   - env var name;
   - UI control;
   - валидатор;
   - runtime consumer(s).

Реестр положить в код как единую декларативную schema/registry, а не оставить только в Markdown.

Нельзя закончить задачу, пока не доказано тестом, что каждый runtime-read field зарегистрирован в этой schema.

---

## Шаг 2. Canonical configuration registry

Создать единый модуль конфигурации:

```text
lt_app/src/runtime_settings.py
```

Он должен содержать:

1. Полную schema всех полей (включая UI widget metadata: тип контрола, label, опции для select).
2. `default_settings()`.
3. `load_env_settings()` — единственное место в проекте, где вызывается `os.environ`/`os.getenv`.
4. `merge_settings(base, patch)` с merge на уровне leaf paths.
5. `load_yaml_settings(text_or_file)`.
6. `dump_yaml_settings(settings)` — сериализует все значения в открытом виде, включая credentials.
7. `validate_settings(settings)`.
8. Маскирование для UI-диагностики/логов (не для export!) без изменения исходного `effective_settings`.
9. Функцию получения строгой immutable runtime-конфигурации для worker (`run_settings`).

Не использовать множество независимых словарей дефолтов в `app.py` и runtime-модулях.

**Обязательное удаление:** `MODULE_DEFAULTS` и `WIDGET_SPECS` в текущем `app.py` — это параллельная, вручную поддерживаемая структура, отдельная от pydantic-схем модулей. Оба словаря должны быть удалены из `app.py`. UI widget metadata (тип контрола, label, опции) переносится в `runtime_settings.py` как часть field descriptor каждого поля в registry. `app.py` не должен содержать собственного перечня полей — он строит форму, итерируясь по registry.

### Имена ENV

Для каждого поля задать стабильное и документированное имя.

Формат:

```text
LT_APP__<SECTION>__<FIELD>
```

Примеры:

```text
LT_APP__GENERATOR__BOOTSTRAP_SERVERS
LT_APP__GENERATOR__TOPIC
LT_APP__GENERATOR__SOURCE_CSV_PATH
LT_APP__COLLECTOR__BOOTSTRAP_SERVERS
LT_APP__COLLECTOR__TOPIC
LT_APP__MONITOR__HOST
LT_APP__MONITOR__PORT
LT_APP__MONITOR__USERNAME
LT_APP__MONITOR__PASSWORD
LT_APP__MONITOR__USE_SSL
```

Значения ENV должны корректно приводиться к schema type: `bool`, `int`, `float`, `str`, список и т. д.

Это единственный слой, где допустим prefix `LT_APP__...`. Старые env-префиксы модулей (`KAFKA_EVENT_GENERATOR_`, `OPENSEARCH_INDEX_MONITOR_`, и т. п.), привязанные к `BaseSettings`, должны быть удалены вместе с удалением `BaseSettings` из этих модулей.

---

## Шаг 3. Строгое правило precedence (временное, не иерархическое)

При инициализации новой не запущенной сессии применяется порядок операций:

```text
defaults
  → ENV values (Apply environment)
  → imported YAML values, если YAML импортирован
  → UI edits
```

Это порядок **типичных операций во времени**, не фиксированная иерархия приоритетов источников. Правило разрешения конфликтов:

> Последняя по времени применённая операция по конкретному field path побеждает — независимо от того, из какого источника (ENV/YAML/UI) она пришла.

Из этого следует:

- Применение YAML — patch merge на leaf-уровне, а не замена всего state.
- Применение ENV — patch merge на leaf-уровне, а не замена всего state.
- UI on-change — обновляет только свой field path, немедленно отражается в `effective_settings`.
- YAML import не должен очищать значения других полей, если импортируемый YAML их не содержит.
- Если пользователь повторно нажимает `Apply environment` после UI-правок, ENV patch снова накатывается и победит для тех полей, что в нём заданы — это ожидаемое, а не ошибочное поведение.
- Нужен явный action `Reset to defaults`, который действительно пересобирает state из defaults.
- Нужен явный action `Apply environment`, который применяет env patch поверх текущего state; это не должно происходить неявно на каждом Streamlit rerun.
- Пока НТ не запущено, UI обязан показывать именно resolved `effective_settings`, обновлённый сразу после любой из операций выше.
- После старта UI может быть read-only/disabled для run-related настроек, но должен отображать immutable `run_settings`, а не значения из нового черновика.

---

## Шаг 4. UI

UI должен строиться из canonical registry, а не иметь вручную поддерживаемый частичный перечень полей (`MODULE_DEFAULTS`/`WIDGET_SPECS` удалены — см. шаг 2).

### Требования

1. Каждый canonical field имеет UI control, определённый в registry.
2. Тип контрола соответствует типу поля:
   - `str` → text input/text area;
   - `int`, `float` → number input;
   - `bool` → checkbox/toggle;
   - list → editor либо стабильный string representation с двусторонним parser;
   - password → password field, значение при этом хранится в registry как обычный `str` (см. инвариант 5).
3. Виджет получает исходное значение из `effective_settings`.
4. Любое изменение виджета записывает значение обратно в `effective_settings` — сразу, по конкретному field path.
5. Не использовать два независимых состояния: одно в `st.session_state`, другое в YAML-text-editor.
6. Если YAML отображается как текстовый editor, он обязан:
   - при валидном ручном редактировании обновлять `effective_settings`;
   - после UI edits отображать актуальный YAML;
   - не быть отдельным конфликтующим source of truth.
7. При YAML upload/import:
   - parse;
   - validate;
   - leaf-level merge (patch, не replace);
   - обновить `effective_settings`;
   - обновить все UI controls;
   - показать структурные ошибки с field path.
8. Кнопка Export YAML выгружает canonical serialization текущего `effective_settings`, полностью, в открытом виде.
9. Нельзя скрывать или вырезать креды из YAML export. Это осознанное требование для внутренней developer-тулзы.

---

## Шаг 5. Runtime-компоненты

### Общие требования

- Runtime-модули принимают typed config (обычный `pydantic.BaseModel`, не `BaseSettings`) или immutable full config.
- Они не должны читать `os.environ` напрямую и не должны наследоваться от `pydantic_settings.BaseSettings`.
- Они не должны использовать собственные defaults для пользовательских настроек — все defaults живут только в `runtime_settings.py`.
- Они не должны менять `run_settings`.

### OpenSearch monitor

- Использовать прямые поля:
  ```yaml
  monitor:
    username: "..."
    password: "..."
  ```
- Не использовать `username_env`/`password_env` в runtime contract.
- Хранить `password` в самом settings-объекте модуля как `str` (не `SecretStr`) на уровне registry/`effective_settings`; допустимо оборачивать в `SecretStr` внутри `client_options()` непосредственно перед созданием OpenSearch client, если это нужно для защиты от случайного логирования внутри модуля — но не выше этого уровня.
- Не логировать пароль и не включать его в exception text вручную.
- При ошибке авторизации выдавать понятное сообщение без credential value.

### Worker

- Перед стартом строит `run_settings = deep_copy(effective_settings)`.
- Валидирует `run_settings`.
- Передаёт один и тот же snapshot в generator, collector, monitor и report stages.
- Не читает ENV.
- Не модифицирует settings по ходу запуска.
- Сохраняет `run_settings.yaml` как артефакт конкретного запуска.

---

## Шаг 6. YAML и ENV документация

Обновить:

- `settings_template.yaml`;
- `session_settings_example.yaml`;
- `.env.example`;
- README/HANDOFF, если там указан старый способ настройки.

### YAML

Оба YAML-файла должны содержать **все canonical fields**, в актуальной структуре и с валидными типами.

В `session_settings_example.yaml` разрешено указывать developer-test credentials как пример полей, но не реальные рабочие секреты репозитория.

Нельзя оставлять обязательные скрытые env-зависимости, если поле уже присутствует в YAML.

### ENV

`.env.example` обязан иметь запись для каждого canonical field с тем же именем `LT_APP__<SECTION>__<FIELD>`, что объявлено в registry. Старые env-имена модулей (`KAFKA_EVENT_GENERATOR_*`, `OPENSEARCH_INDEX_MONITOR_*` и т. п.) должны быть удалены как из кода, так и из документации.

---

## Шаг 7. Обязательные тесты

Добавить тесты. Использовать реальные schema paths и реальные runtime consumers.

### Покрытие registry

1. Тест обнаруживает любое runtime-конфигурационное поле, которое:
   - используется в runtime-коде;
   - отсутствует в canonical registry.
2. Тест проверяет, что для каждого registry field существуют:
   - YAML mapping;
   - ENV mapping (имя вида `LT_APP__<SECTION>__<FIELD>`);
   - UI metadata/control registration;
   - default;
   - type parser/validator.
3. Тест проверяет отсутствие `pydantic_settings.BaseSettings` (или `env_prefix`/`env_file`) во всех runtime-модулях из границ задачи.
4. Тест проверяет отсутствие `SecretStr` (или эквивалента, скрывающего значение при `str()`/`repr()`/`yaml.safe_dump`) в самом `effective_settings`/registry-слое.

### Слияние источников

5. `defaults + ENV + YAML + UI`:
   - каждая операция меняет только свои leaf fields;
   - precedence — временной: последняя по времени операция по field path побеждает, независимо от типа источника;
   - разные поля могут иметь разные источники одновременно.
6. Частичный YAML и частичные UI edits:
   - YAML задаёт половину полей;
   - UI задаёт вторую половину;
   - export содержит обе половины и все остальные default/resolved поля.
7. ENV-значение:
   - становится частью `effective_settings` после `Apply environment`;
   - появляется в YAML export в открытом виде;
   - отображается соответствующим UI control.
8. Повторное применение ENV (`Apply environment`) после UI-правок перекрывает поля, заданные в ENV, даже если они были ранее переопределены в UI — так как эта операция происходит позже по времени.

### Round-trip и state synchronization

9. `YAML import → UI edit → YAML export → clean import`:
   - восстановленный state равен state до export.
10. `UI edit → export → import`:
   - состояние не теряет ни одно поле.
11. Импорт неполного YAML:
   - меняет только явно переданные paths;
   - другие значения не очищаются.
12. Некорректный YAML или некорректный type:
   - не портит существующий `effective_settings`;
   - выдаёт понятную ошибку с canonical path.

### Runtime snapshot

13. После start:
   - worker получает exact deep-copied `run_settings`;
   - последующие UI changes не меняют `run_settings`.
14. Ни generator, ни collector, ни monitor не используют `os.getenv()` для runtime settings и не наследуются от `BaseSettings`.
15. Сохранённый артефакт `run_settings.yaml` точно соответствует тому, что получил worker, включая credentials в открытом виде.

---

## Acceptance criteria

Задача завершена только если одновременно выполнены все условия:

1. Полный registry включает каждую реально используемую runtime-настройку.
2. Каждая настройка доступна через YAML, ENV и UI.
3. До запуска существует единственный `effective_settings`.
4. Экспорт всегда содержит полный effective state на текущую секунду, в открытом виде, включая credentials.
5. Частичный YAML + частичный UI + ENV корректно объединяются field-by-field по временному precedence.
6. UI всегда показывает фактический pre-run effective state без задержки.
7. Worker запускается только с immutable `run_settings`.
8. YAML и ENV examples полностью покрывают registry с именами `LT_APP__<SECTION>__<FIELD>`.
9. Все перечисленные тесты проходят.
10. Нет неявного runtime-чтения environment variables, нет `BaseSettings`/`env_prefix` в runtime-модулях, нет `SecretStr` в registry-слое, нет скрытых конфигурационных default-значений вне canonical registry, `MODULE_DEFAULTS`/`WIDGET_SPECS` удалены из `app.py`.

---

## Отчёт агента

В конце работы агент обязан выдать краткий отчёт:

1. Список изменённых файлов.
2. Полный перечень canonical sections/fields.
3. Список удалённых неявных ENV/fallback-механизмов (включая явное подтверждение удаления `BaseSettings`/`env_prefix` из каждого модуля и удаления `MODULE_DEFAULTS`/`WIDGET_SPECS` из `app.py`).
4. Результаты тестов с точной командой запуска.
5. Подтверждение каждого acceptance criterion.
6. Известные ограничения — только если они подтверждены кодом и не противоречат требованиям выше.

Не предлагать альтернативную архитектуру вместо выполнения этой задачи.
Не расширять scope.
Не изменять Kafka/ETL бизнес-логику.
Не выполнять production-hardening вместо developer usability.
