# Data Model: `flow init`

| Entity | Fields | Rules |
|---|---|---|
| request | name, language (`scala` default, `python`), directory (default the name), package (optional) | name passes the streamlet-name rule and starts with a letter; package valid for the language and only for it; the directory absent or empty |
| template | language, files (each a path and contents, both may hold tokens), index | the index lists exactly the files; a path written twice fails the build |
| tokens | name, class, package, package_path, module, flow_version, sdk_version, protocol_version | derived from the request and the CLI's build; sdk_version is flow_version at a release, `0.0.0` otherwise |
| project | the rendered files in the directory | no `{{token}}` left; the descriptor equals what the SDK writes; the blueprint verifies |

States of a run: **parsed** → **refused** (nothing written, exit 2 with the reason) or **rendered**
(every file written, exit 0, the next commands printed).
