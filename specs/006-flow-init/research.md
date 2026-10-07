# Research: `flow init`

## R1. Templates as resources with an index, ankka's embedded path

**What is there**: ankka carries its Python, TypeScript, Rust and web templates in its CLI jar under
`ankka/templates/<language>/`, assembled by an sbt `resourceGenerators` task from
`cli/src/main/templates/<language>/` plus shared files, with an `index.txt` per language because a
directory inside a jar or a native image cannot be listed. Rendering is exact token replacement in
paths and contents. Its Scala template is a separate Giter8 repository fetched by `sbt new`, which
needs sbt and the network and is not pinned to the CLI's version.

**Decision**: both languages use the embedded path. Sources live in `cli/src/main/templates/scala/`
and `cli/src/main/templates/python/`; files shared by both (the compose file, the sidecar's
configuration, the blueprint, the cluster configuration, `.gitignore` lines) in
`cli/src/main/templates/common/`. A `resourceGenerators` task copies them, and the ankka-flow
plugin's rendered skills from `marketplace/plugins/ankka-flow/skills/` as `.claude/skills/`, into
`ankka-flow/templates/<language>/` with an `index.txt`, walking hidden files by hand (sbt's default
filter drops them) and failing the build on a path written twice (FR-016). The native image
registers `ankka-flow/templates/**` in the CLI's reachability metadata.

**Alternatives considered**: Giter8 — rejected above. `sbt new`/`uv init` — produce someone else's
project, not one with a streamlet, descriptor and blueprint. Templates generated in code — every file
becomes a string in Scala, unreadable and unreviewable.

## R2. Tokens

**Decision**: exact `{{token}}` replacement in paths and contents, no expressions, no escaping
(GitHub's `${{ }}` passes through because it never matches a token):

| Token | Value | Example for `order-greeter` |
|---|---|---|
| `{{name}}` | the streamlet and pipeline name | `order-greeter` |
| `{{class}}` | the name in UpperCamelCase | `OrderGreeter` |
| `{{package}}` | Scala package (`--package`, default the name without hyphens) | `ordergreeter` |
| `{{package_path}}` | the package as a path | `ordergreeter` |
| `{{module}}` | Python module (`--package`, default the name with `_` for `-`) | `order_greeter` |
| `{{flow_version}}` | the `flow` that wrote it | `0.5.0` |
| `{{sdk_version}}` | what the SDK reports at that version: `flow_version` for a release, `0.0.0` otherwise | `0.5.0` |
| `{{image_version}}` | `flow_version` as a Docker tag (`+` becomes `-`) | `0.5.0` |
| `{{fingerprint}}` | the contract fingerprint of `<name>.v1` | `…=` |
| `{{scala_version}}` | the Scala 3 LTS the SDK is published for, from the build | `3.3.8` |
| `{{protocol_version}}` | the protocol the CLI writes | `1.0` |
| `{{sbt_version}}` | the sbt this repository builds with, from the build | `1.12.15` |
| `{{native_packager_version}}` | the sbt-native-packager this repository uses, from the build | `1.11.7` |

**Refusals (FR-002)**: the name must pass `DescriptorValidation`'s streamlet-name rule *and* start
with a letter, because it becomes a class, a package and a module; a name starting with a digit is
refused, whatever `--package` says, because the class name is always derived from the name.
`--package` must be a dotted lower-case identifier (Scala) or a Python identifier (Python). `--dir`
must not exist or be empty. An unknown language is refused by decline.

## R3. Versions

**Decision**: the project depends on the SDK at the CLI's version: `"com.thinkmorestupidless" %%
"ankka-flow-sdk" % "{{flow_version}}"` and `ankka-flow=={{sdk_version}}` (equal at a release; between
releases the Python pin is `0.0.0`, the version this repository's Python SDK has in its tree, because
a dynver version is not a valid Python version); the compose file runs
`ghcr.io/thinkmorestupidless/ankka-flow-sidecar:{{image_version}}`. The committed descriptor's SDK
block says `{{sdk_version}}`, which equals what the published SDK of that version reports — its
version at a release, `0.0.0` from any other build, the same rule the Scala SDK's build info and the
Python SDK's tree follow. A `flow` built between releases names a version no registry holds; the
README says so, and the template suite builds against this repository's SDK instead (R6).

## R4. The descriptor is a template file

**Decision**: each language's template carries `flow/descriptor.json` with tokens, in canonical form
(keys sorted, the protocol's printer's output for the template streamlet). The project's own
descriptor command and check prove it equal to what the SDK writes (FR-007); the template suite runs
that check, so a template whose descriptor and code disagree fails the build. The descriptor names
the SDK (`ankka-flow-scala` or `ankka-flow-python`) and `{{sdk_version}}`.

**Alternatives considered**: `flow init` writing the descriptor from a declaration held in the CLI
— a second copy of the streamlet's declaration, in Scala, beside the template's.

## R5. The projects

**Scala**: a standalone sbt build — `build.sbt` (Scala 3.3 LTS, the SDK, munit, `slf4j-simple`,
sbt-native-packager's `JavaAppPackaging` and `DockerPlugin` on `eclipse-temurin:21-jre`, a
`descriptor` and a `descriptorCheck` task as the Scala sample has, `run / fork`),
`project/build.properties` (this repository's sbt), `project/plugins.sbt`,
`src/main/scala/{{package_path}}/{{class}}.scala`, `Main.scala`,
`src/test/scala/{{package_path}}/{{class}}Suite.scala`.

**Python**: `pyproject.toml` (hatchling, `ankka-flow=={{flow_version}}`, pytest in a dev group,
`[tool.ankka-flow] streamlet = "{{module}}.streamlet:{{class}}"`), `src/{{module}}/{streamlet,main}.py`,
`tests/test_streamlet.py`, a `Dockerfile` (`pip install .`, ankka's shape), `.dockerignore`.

**Both**: `blueprint.conf` (`{{name}}`: an unmanaged input topic `{{name}}.in` on `kafka:9092`,
earliest, and a managed output topic `out`), `flow/streamlet.conf` (the sidecar's configuration for
the compose network, the Python sample's shape), `docker-compose.yml` (Kafka with an external
listener on `localhost:9094`, the sidecar at `{{flow_version}}`, the process on
`host.docker.internal:9010`), `k8s/in-cluster.conf`, `README.md`, `.gitignore`,
`.github/workflows/ci.yml` (tests and the descriptor check), `.claude/skills/`.

**The streamlet** (clarified): reads a JSON object, adds `greeting` = the parameter `greeting`
(default `hello, ankka-flow`), emits it with the record's key and headers; a value that is not a
JSON object fails the batch. Its test: one record gets the greeting; the parameter's configured
value is used; a non-object value fails the batch.

**The laptop loop** (clarified): the README creates the topics and produces and consumes with
Kafka's console tools via `docker compose exec kafka /opt/kafka/bin/…`, identical for both
languages.

## R6. The proof

**Decision**:

- `InitSuite` (unit, always): every refusal of R2 and the target directory, each leaving nothing
  written; rendering replaces every token and leaves none behind (`{{` absent from every file but
  the workflow's `${{`); both languages' file lists equal their index; a project carries the skills.
- `TemplateSuite` (gated by `-Dflow.template.tests=scala,python|off`, forwarded; on by default in
  `sbt test`, as the k3s suite is): for each language, render through `Main.run` into a temporary
  directory, then
  - Scala: `sdk/publishLocal` and `protocol/publishLocal` first (the CLI's version is the SDK's in
    this build), so the project's dependency resolves as written; run the project's `sbt test
    descriptorCheck Docker/publishLocal` as a subprocess;
  - Python: add `[tool.uv.sources] ankka-flow = { path = "<repo>/sdks/python" }` to the rendered
    `pyproject.toml`, then `uv sync`, `uv run pytest -q`, `uv run descriptor --check`; the image is
    built from a second copy rendered at the latest released SDK version, so its unmodified
    Dockerfile installs from PyPI as a reader's does (a development version is on no registry);
  - both: `flow verify blueprint.conf --descriptors flow` through `Main.run` passes.
- `native-smoke.sh` gains init for both languages, a file-list check against the index, and a byte
  `cmp -r` against the JVM build's projects (SC-003).
- The laptop round trip (SC-002) is quickstart tier 3, run by hand once, as 005's was.

## R7. Docs

**Decision**: the first-streamlet tutorial becomes "`flow init`, then this" with the sample kept as
the worked example for the laptop loop's details; the install page's next step is `flow init`;
`reference/cli.md` documents the command; the contributing page says where templates live and how
the index is built; the skills say `flow init` is how a project starts.

## Verify first

1. The `resourceGenerators` walk carries `.gitignore`, `.dockerignore`, `.github/` and
   `.claude/` into the jar and the native image.
2. The rendered Scala project resolves the locally published SDK by its snapshot version.
3. The Python `Dockerfile` builds with `pip install .` against the path SDK in the suite (the image
   built in the suite installs the SDK from the repository, not PyPI).
