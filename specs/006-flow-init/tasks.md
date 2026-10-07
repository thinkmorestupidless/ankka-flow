# Tasks: `flow init`

**Input**: Design documents from `/specs/006-flow-init/`

**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md), [research.md](./research.md),
[data-model.md](./data-model.md), [contracts/flow-init.md](./contracts/flow-init.md),
[quickstart.md](./quickstart.md); the scenarios are in `features/cli/init.feature` and
`features/cli/install-page.feature`.

**Tests**: included, first where a test can be written before the thing it holds. The real proof is
the template suite: a template that renders but does not build is the failure that matters.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: can run in parallel — different files, no dependency on an incomplete task
- **[Story]**: US1 (a working project from one command), US2 (refusals that name the problem), US3
  (from the project to the laptop and the cluster), US4 (in the native binary, and in the docs)

`CLI` = `cli/src/main/scala/com/thinkmorestupidless/ankka/flow/cli`, `CLIT` =
`cli/src/test/scala/com/thinkmorestupidless/ankka/flow/cli`, `TPL` = `cli/src/main/templates`. "R*n*"
is a section of `research.md`, "V*n*" an item of its *Verify first* list.

---

## Phase 1: Setup

- [X] T001 Add `flow.template.tests` to `forwardedTestSwitches` in `build.sbt` with a comment (`scala,python` by default, a comma list, or `off`); create `TPL/common/`, `TPL/scala/`, `TPL/python/` each holding one placeholder file so the generator has input.
- [X] T002 In `build.sbt`, a `Compile / resourceGenerators` task on `cli` (R1): for each language, copy `TPL/common/**` then `TPL/<language>/**` (walking hidden files by hand) and `marketplace/plugins/ankka-flow/skills/**` as `.claude/skills/**` into `resourceManaged/ankka-flow/templates/<language>/`, write `index.txt` (sorted relative paths), and fail with both sources named when a path is written twice; `unzip -l cli/target/…jar` shows `.gitignore`, `.github/` and `.claude/` (V1). Commit Phase 1.

**Checkpoint**: the CLI jar carries both template trees and their indexes.

---

## Phase 2: Foundational — the request, the tokens, the renderer (R2, R3)

**⚠️ CRITICAL**: every story depends on this phase.

- [X] T003 Write `CLIT/InitSuite.scala` first (red): rendering a two-file test template replaces every token in paths and contents and leaves `${{ github.sha }}` as it is; `{{class}}` of `order-greeter` is `OrderGreeter`, `{{package}}` `ordergreeter`, `{{module}}` `order_greeter`; `{{sdk_version}}` is the CLI's version at a release and `0.0.0` for a version with `+` or `SNAPSHOT`; `{{sbt_version}}` and `{{native_packager_version}}` are the build's own; each language's rendered file list equals its `index.txt`; a rendered project holds `.claude/skills/ankka-flow*/SKILL.md` byte-equal to the plugin's (C3).
- [X] T004 Write `CLI/Init.scala`: `Init.Request(name, language, dir, packageName)`, `Language` (`Scala`, `Python`), `Init.problems(request): Vector[String]` per R2, and `Init.tokens(request, version, protocolVersion): Map[String, String]`.
- [X] T005 Write `CLI/Scaffold.scala`: read `ankka-flow/templates/<language>/index.txt` from the classpath, render each path and its contents with exact `{{token}}` replacement, write under the directory (creating parents, preserving nothing else), and return the paths written; refuse to write if any rendered file would still contain `{{` outside `${{`.
- [X] T006 In `CLI/Main.scala`, the `init` subcommand per `contracts/flow-init.md`: `<name>`, `--language`/`-l` (`scala` default), `--dir`, `--package`; refusals to stderr and exit 2 with nothing written; success prints the directory and the next three commands. `InitSuite` green. Commit Phase 2.

**Checkpoint**: `flow init` renders whatever the templates hold; refusals are not yet all tested.

---

## Phase 3: User Story 1 — A working project from one command (Priority: P1)

**Goal**: both templates, rendered, build and pass their own checks against this repository's SDK.

**Independent Test**: `sbt 'cli/testOnly *TemplateSuite'` green for both languages.

- [X] T007 [P] [US1] Write `TPL/common/`: `blueprint.conf` (contract R5: pipeline `{{name}}`, unmanaged `in` = `{{name}}.in` on `kafka:9092` earliest, managed `out`), `flow/streamlet.conf` (the Python sample's shape for `{{name}}`, topics `{{name}}.in` and `{{name}}.out`), `docker-compose.yml` (the cart router's compose with the sidecar image `ghcr.io/thinkmorestupidless/ankka-flow-sidecar:{{flow_version}}`), `k8s/in-cluster.conf`, `.gitignore` (both languages' build outputs).
- [X] T008 [P] [US1] Write `TPL/scala/`: `build.sbt` (Scala 3.3.8, `"com.thinkmorestupidless" %% "ankka-flow-sdk" % "{{flow_version}}"`, munit, slf4j-simple, `JavaAppPackaging` and `DockerPlugin` on `eclipse-temurin:21-jre` with no exposed port, `run / fork`, `descriptor` and `descriptorCheck` tasks as `cartRouterScala` has them), `project/build.properties` (`sbt.version={{sbt_version}}`), `project/plugins.sbt` (sbt-native-packager at `{{native_packager_version}}`), `src/main/scala/{{package_path}}/{{class}}.scala` (the greeting streamlet per the contract, using the protocol's `Json`), `src/main/scala/{{package_path}}/Main.scala`, `src/test/scala/{{package_path}}/{{class}}Suite.scala` (greeting added with key and headers kept; the configured greeting; a non-object value fails the batch), `flow/descriptor.json` (canonical, SDK `ankka-flow-scala` `{{sdk_version}}`).
- [X] T009 [P] [US1] Write `TPL/python/`: `pyproject.toml` (hatchling, `ankka-flow=={{flow_version}}`, pytest in a dev group, `[tool.ankka-flow] streamlet = "{{module}}.streamlet:{{class}}"`), `src/{{module}}/__init__.py`, `src/{{module}}/streamlet.py`, `src/{{module}}/main.py`, `tests/test_streamlet.py` (the same three tests), `Dockerfile` (ankka's shape: `pip install .`, `CMD ["python", "-m", "{{module}}.main"]`), `.dockerignore`, `flow/descriptor.json` (SDK `ankka-flow-python` `{{sdk_version}}`).
- [X] T010 [US1] Write `CLIT/TemplateSuite.scala` gated by `flow.template.tests`: for each enabled language, render through `CliFixtures.flow("init", …)` into a temporary directory; for Scala, make the suite depend on `sdk/publishLocal` and `protocol/publishLocal` in `build.sbt` (`Test / test` and `testOnly` of `cli`, only when the switch enables Scala) and run `sbt -batch test descriptorCheck` in the project; for Python, append `[tool.uv.sources] ankka-flow = { path = "<repo>/sdks/python", editable = true }` and run `uv sync`, `uv run pytest -q`, `uv run descriptor --check`; for both, `flow verify blueprint.conf --descriptors flow` exits 0, and the rendered `.github/workflows/ci.yml` passes `actionlint` when it is installed, or else parses as YAML with a step running the tests and a step running the descriptor check. V2 and V3. Green.
- [X] T011 [US1] Commit Phase 3.

**Checkpoint**: SC-001 holds for both languages.

---

## Phase 4: User Story 2 — Refusals that name the problem (Priority: P1)

**Goal**: every refusal of R2 and the directory rule, tested, leaving nothing written.

**Independent Test**: `InitSuite`'s refusal cases.

- [X] T012 [US2] Add to `CLIT/InitSuite.scala` one test per row of `features/cli/init.feature`'s refusal outline and the edge cases: a capital, longer than 63, a digit first, a trailing hyphen, an empty name, a non-empty directory (an existing empty directory is used), `--package` invalid for Scala and for Python, a language that does not exist; each asserts exit 2, the rule in the message, and that the directory was not created or was left as found. Green; commit.

**Checkpoint**: US2 holds.

---

## Phase 5: User Story 3 — From the project to the laptop and the cluster (Priority: P2)

**Goal**: the README's commands, the image, the CI workflow.

**Independent Test**: the template suite builds both images; quickstart tier 3 by hand.

- [ ] T013 [P] [US3] Write `TPL/scala/README.md` and `TPL/python/README.md`: what the project is, the commands (test, descriptor and its check, `flow verify`), the laptop loop with Kafka's console tools through `docker compose exec kafka /opt/kafka/bin/…` (create `{{name}}.in`, run the streamlet on the host, produce `{"id": 1}` keyed, consume `{{name}}.out`), the image, deploying to kind with `flow generate blueprint.conf --descriptors flow --conf k8s/in-cluster.conf --image {{name}}=<image> -n <namespace>`, the version note (R3), and where the skills are.
- [ ] T014 [P] [US3] Write `TPL/scala/.github/workflows/ci.yml` and `TPL/python/.github/workflows/ci.yml`: on push and pull request, set up the language, run the tests and the descriptor check.
- [ ] T015 [US3] Extend `CLIT/TemplateSuite.scala`: build each project's image and assert it exposes no port (`docker image inspect`). Scala: `sbt -batch Docker/publishLocal` in the project already built against the locally published SDK. Python: render a second copy by calling `Scaffold` directly with `flow_version` set to the latest released SDK version (read from a constant the suite names, today `0.4.1`), so its unmodified Dockerfile installs `ankka-flow==<released>` from PyPI exactly as a reader's does; `docker build -t <name>:test .`. The project's tests, descriptor check and `flow verify` stay on the repository's SDK (T010). Green.
- [ ] T016 [US3] Run quickstart tier 3 by hand for both languages with the current build (the compose file's sidecar tag overridden to `latest` from `sbt sidecar/docker:publishLocal`): the greeting reaches `{{name}}.out`. Also pull the released sidecar image anonymously (`docker logout ghcr.io; docker pull ghcr.io/thinkmorestupidless/ankka-flow-sidecar:<latest release>`): if it is refused, the packages are private, which only the maintainer can change in the repository's package settings; say so in the README until they are public. Record both in `research.md`. Commit Phase 5.

**Checkpoint**: SC-002 observed.

---

## Phase 6: User Story 4 — In the native binary, and in the docs (Priority: P2)

**Goal**: the native binary writes the same projects; the docs start from `flow init`.

**Independent Test**: `just cli-native`; `just docs`.

- [ ] T017 [US4] Add `ankka-flow/templates/**` to `cli/src/main/resources/META-INF/native-image/com.thinkmorestupidless/ankka-flow-cli/reachability-metadata.json` and its README's list of hand-added entries.
- [ ] T018 [US4] In `cli/native-smoke.sh`, run `init` for both languages with the binary and, when given the JVM `flow`, with it too; check each project's files against the index and `cmp -r` the two trees (SC-003); time the binary's init (SC-004). `just cli-native` green.
- [ ] T019 [P] [US4] Docs: `docs/get-started/first-streamlet.md` starts with `flow init` (both tabs), keeping the cart router as the worked laptop loop; `docs/get-started/install.md`'s next step is `flow init`; `docs/reference/cli.md` documents `init` per the contract; `docs/contributing/building.md` says where templates live and how the index is built; `tools/docs/skill/ankka-flow*/SKILL.md` say a project starts with `flow init`. `just docs-sync && just docs` clean.
- [ ] T020 [US4] In `.github/workflows/ci.yml`, the `cli-native` filter and the `build` filter include `cli/src/main/templates/**` and `marketplace/plugins/ankka-flow/skills/**`; in `CLAUDE.md`, the commands block names `flow init`'s suites and the template switch, and a rule says templates are plain files the build indexes, the descriptor in each is checked by the template suite, and a template change is a docs change. Commit Phase 6.

**Checkpoint**: SC-003, SC-004 and the docs hold.

---

## Phase 7: Polish

- [ ] T021 Run the whole build (`caffeinate -i sbt scalafmtCheckAll scalafmtSbtCheck test mutationCheck` with both template languages, the Python SDK's checks, `just features`, `just docs`, the README check, `just cli-native`) and tick the reviewer's checklist in `specs/006-flow-init/quickstart.md`.
- [ ] T022 Write "Found during implementation" in `specs/006-flow-init/research.md` (each V-item's answer) and the pull request description.

---

## Dependencies & Execution Order

- **Phase 1 → 2 → 3**: the generator (T002) before the renderer reads it (T005); the renderer before any template is proven (T010).
- **Phase 3**: T007, T008, T009 in parallel; T010 after all three.
- **Phase 4** needs Phase 2 only; it can go beside Phase 3.
- **Phase 5**: T013 and T014 in parallel after Phase 3; T015 after T010.
- **Phase 6**: T017 before T018; T019 after Phase 5 (the README's commands are what the tutorial shows); T020 last.
- **Phase 7** after everything.

### Parallel opportunities

T007, T008, T009; T012 beside Phase 3; T013, T014; T019 beside T017–T018.

## Implementation Strategy

**MVP is US1 with Phases 1–2**: `flow init` writing a Scala and a Python project that build and pass
their own checks against this repository's SDK. Everything after is refusals, the laptop loop, the
native binary and the docs on top of a proven template.

### Notes

- A template is changed only with the template suite run for its language.
- The descriptor in a template is never written by hand twice: after a change to the streamlet, run
  the project's descriptor command in a rendered copy and copy the result back with tokens.
