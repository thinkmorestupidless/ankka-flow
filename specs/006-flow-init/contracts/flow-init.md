# Contract: `flow init`

```text
flow init <name> [--language scala|python] [--dir <directory>] [--package <package>]
```

| Flag | Default | Meaning |
|---|---|---|
| `<name>` | — | the streamlet and pipeline name; 1–63 of `[a-z0-9-]`, starting with a letter, not ending with `-` |
| `--language`, `-l` | `scala` | `scala` or `python` |
| `--dir` | `<name>` | where to write; must not exist or be empty |
| `--package` | Scala: the name without `-`; Python: the name with `_` for `-` | the Scala package (dotted, lower case) or the Python module |

Exit 0 and, on stdout, the directory written and the next three commands (test, descriptor check,
`flow verify`). Exit 2 and, on stderr, one line per refusal; nothing written.

## What is written

| Path | Scala | Python |
|---|---|---|
| streamlet | `src/main/scala/<package>/<Class>.scala` | `src/<module>/streamlet.py` |
| entry point | `src/main/scala/<package>/Main.scala` | `src/<module>/main.py` |
| test | `src/test/scala/<package>/<Class>Suite.scala` | `tests/test_streamlet.py` |
| build | `build.sbt`, `project/build.properties`, `project/plugins.sbt` | `pyproject.toml` |
| image | sbt-native-packager (`sbt Docker/publishLocal`) | `Dockerfile`, `.dockerignore` |
| descriptor | `flow/descriptor.json` | `flow/descriptor.json` |
| descriptor command | `sbt descriptor`, `sbt descriptorCheck` | `uv run descriptor`, `uv run descriptor --check` |
| both | `blueprint.conf`, `flow/streamlet.conf`, `docker-compose.yml`, `k8s/in-cluster.conf`, `README.md`, `.gitignore`, `.github/workflows/ci.yml`, `.claude/skills/**` | the same |

## The streamlet

One inlet `in` and one outlet `out`, both JSON with schema name `<name>.v1`; one string parameter
`greeting`, default `hello, ankka-flow`. Each record's value is read as a JSON object; the object
with `"greeting": <greeting>` added is emitted to `out` with the record's key and headers. A value
that is not a JSON object fails the batch.

## The blueprint

Pipeline `<name>`; streamlet `<name>` = descriptor `<name>`; topic `in`: unmanaged, `<name>.in` on
`kafka:9092`, consumed by `<name>.in`, earliest; topic `out`: managed, produced by `<name>.out`,
3 partitions, replication 1.
