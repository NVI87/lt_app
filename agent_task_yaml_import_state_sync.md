# Task: Make Streamlit YAML import reliably update session settings

## Goal

Fix the Streamlit configuration-import flow so that a YAML file uploaded in the sidebar becomes the effective configuration for the current test session, is visibly reflected in all editable sidebar widgets, passes existing Pydantic validation, and is the configuration supplied to `SessionWorker` after **Start Test**.

The desired Docker UX is:

1. Start the image with no mounted YAML configuration file.
2. Open the Streamlit UI.
3. Upload a `.yaml` or `.yml` file in **Import settings from YAML**.
4. Click **Apply imported settings**.
5. See the imported values in the corresponding sidebar fields.
6. See validation errors if the imported effective settings are invalid.
7. Click **Start Test** and run with the imported settings.

## Scope

Primary file:

- `lt_app/app.py`

Add focused tests only if a suitable test location and import strategy already exist in the repository. The change must not modify the worker lifecycle, Kafka/OpenSearch logic, report aggregation logic, Docker files, package metadata, dependency files, or documentation.

Do not add secret-management functionality. YAML remains session configuration; credentials must continue to be supplied by the existing environment/settings mechanism.

## Problem to fix

The current code has:

- `st.sidebar.file_uploader("Import settings from YAML", ...)`;
- `apply_yaml_import(settings_dict, yaml_data)` which updates `st.session_state.settings`;
- sidebar widgets with persistent keys in the form `settings_{module_name}_{field}`;
- widget rendering that assigns each widget return value back into `st.session_state.settings[module_name][field]`.

Because persistent widget values can survive Streamlit reruns, updating only the nested `settings` dictionary can be overwritten by stale widget state on the next render. A YAML import must synchronise the canonical nested settings dictionary and every relevant widget state entry before those widgets are instantiated in that rerun.

## Required implementation

### 1. Preserve the existing YAML schema

Keep the current top-level module keys exactly as defined by `MODULE_DEFAULTS`:

- `generator`
- `monitor`
- `collector`
- `statistics`
- `projection`
- `stage_size`
- `throughput`

Unknown top-level YAML keys must not be copied into runtime settings. A known module key whose value is not a mapping must produce the existing warning behavior and be skipped.

The import remains a merge over defaults/current settings: absent module keys and absent fields retain their current values.

### 2. Synchronise widget state

When applying an imported YAML mapping, for every accepted imported field:

```python
widget_key = f"settings_{module_name}_{field}"
```

Set both:

```python
st.session_state.settings[module_name][field]
st.session_state[widget_key]
```

Do not create widget-state entries for unknown fields. Only fields declared in `WIDGET_SPECS[module_name]` are UI-controlled settings and may receive a widget-state update.

Handle the `report_interval_minutes` control deliberately:

- It is not a module setting in the current exported YAML schema.
- Do not add it to the YAML schema as part of this task.
- Do not modify its behavior.

### 3. Ensure correct Streamlit ordering

The mutation of `st.session_state[widget_key]` must occur before the corresponding Streamlit widget is created during the rerun. Do not silence Streamlit API errors with broad `try/except`.

A valid solution may process the import action before rendering the module widgets, or use a proper Streamlit callback that runs before the script body renders widgets. Choose the smallest clear solution compatible with the project style.

The UI should keep an explicit Apply action. Uploading a file alone must not silently change the active settings.

### 4. Read upload bytes safely

Prefer `uploaded_file.getvalue()` over a cursor-consuming `.read()` when parsing the uploaded YAML. Decode as UTF-8 and keep `yaml.safe_load`; do not use unsafe YAML loaders.

Handle a YAML document whose root is `null` or another non-mapping with a user-visible error rather than a traceback.

### 5. Validate after application

After a successful Apply action, call the existing `build_settings_objects(st.session_state.settings)` so that `st.session_state.validation_errors` reflects the imported effective configuration.

Do not change `SessionWorker` construction. The existing Start path must continue to build its settings objects from `st.session_state.settings`.

### 6. User feedback

After a successful Apply action, show a concise success message, for example:

```text
Imported settings applied. Review the sidebar values, then start the test.
```

Keep YAML parsing and validation failures visible in the sidebar. Validation errors may be shown after import; applying an invalid configuration is allowed, but starting the test must remain blocked by the existing validation gate.

## Acceptance criteria

- A valid YAML file uploaded through the sidebar does not require a YAML file mounted inside the Docker container.
- The user must click **Apply imported settings** before imported values take effect.
- After Apply, every imported, known field visibly updates in the corresponding sidebar widget.
- After the normal Streamlit rerun, widget values do not overwrite the imported values with stale pre-import values.
- YAML values are present in `st.session_state.settings` and are used by the existing Start Test path.
- Missing modules/fields preserve the current settings.
- Unknown YAML modules and unknown fields do not become runtime settings or arbitrary Streamlit session-state keys.
- Non-mapping YAML root and non-mapping known modules are handled without an uncaught exception.
- `yaml.safe_load` remains in use.
- Existing export behavior and the current YAML shape remain compatible.
- Existing tests remain green.
- The implementation contains no credentials, real endpoints, sample Kafka payloads, test artifacts, or generated files.

## Suggested verification

Run the repository’s existing focused test suite. Add/run focused tests for pure helper behavior if practical without testing Streamlit internals through a browser.

Then perform a manual smoke test:

1. Start the app.
2. Change at least two sidebar values manually, including one text field and one numeric or boolean field.
3. Upload a YAML containing deliberately different values for those same fields, plus a valid value for another module.
4. Click **Apply imported settings**.
5. Confirm the sidebar displays YAML values after rerun.
6. Export settings to YAML and confirm the exported values match the applied configuration.
7. Confirm an invalid YAML produces a parse error and does not crash the app.
8. Confirm syntactically valid but Pydantic-invalid settings show validation errors and Start Test is blocked.

## Working rules

- Work only in the assigned branch/worktree. Do not commit, push, rebase, merge, or modify Git configuration.
- Before editing, provide a short implementation plan naming the function(s) and execution ordering you will change. Wait for approval if the controlling workflow requires it.
- Keep the patch minimal and within this task’s scope.
- Do not use subagents, background agents, parallel tasks, broad refactors, or unrelated cleanup.
- Do not modify generated artifacts, data fixtures, `.env`, secrets, lock files, Docker configuration, `pyproject.toml`, README, or CI.
- Run relevant checks after implementation and report: changed files, tests/checks run with results, and any remaining limitation.
