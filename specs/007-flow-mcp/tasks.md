# Tasks: `flow mcp`

**Input**: Design documents from `/specs/007-flow-mcp/`

**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md), [research.md](./research.md),
[data-model.md](./data-model.md), [contracts/mcp-tools.md](./contracts/mcp-tools.md),
[quickstart.md](./quickstart.md); the scenarios are in `features/cli/mcp.feature` and one in
`features/cli/init.feature`.

**Tests**: included, first where a test can be written before the thing it holds.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: can run in parallel — different files, no dependency on an incomplete task
- **[Story]**: US1 (the docs and the pure tools), US2 (a pipeline read), US3 (a pipeline
  changed), US4 (connected without configuration)

`CLI` = `cli/src/main/scala/com/thinkmorestupidless/ankka/flow/cli`, `MCP` = `CLI/mcp`, `CLIT` =
`cli/src/test/scala/com/thinkmorestupidless/ankka/flow/cli`, `ANKKA` =
`../ankka/cli/src/main/scala/com/thinkmorestupidless/ankka/cli/mcp`. "R*n*" is a section of
`research.md`, "V*n*" an item of its *Verify first* list.

---

## Phase 1: Setup — the docs inside the binary (R2)

- [X] T001 In `build.sbt`, a `Compile / resourceGenerators` task on `cli` copying `docs/**/*.md` to `resourceManaged/ankka-flow/docs/<path>` with an `index.txt`, beside `initTemplates`; in `cli/src/main/resources/META-INF/native-image/…/reachability-metadata.json`, the glob `ankka-flow/docs/**`, noted in its README. `unzip -l` shows the pages.

---

## Phase 2: Foundational — the server and the pages (R1, R2)

**⚠️ CRITICAL**: every story depends on this phase.

- [X] T002 Write `CLIT/McpServerSuite.scala` first (red), with a `talk(messages*)` helper driving `McpServer.handleLine`: `initialize` with each supported version and an unknown one (newest wins); `ping`; a notification answered with nothing; a non-JSON line answered with a parse error; a batch; `tools/list` has every tool with a description, a non-empty `inputSchema` and `annotations`; `resources/list` has every page of the CLI's index; `resources/read` of one returns its Markdown; `tools/call` of an unknown tool is an invalid-params error.
- [X] T003 [P] Add to the protocol module's `Json` what the server needs (V1): `Json.obj(fields*)`, `Json.arr`, `str`, `num`, `bool` constructors, `apply(field)`, `string(field)`, `int(field)`, `fields` on `Obj`, and a test in `protocol/…/JsonSuite.scala` round-tripping a JSON-RPC message with a numeric and a string id, nested objects and escaped strings — only if `protocol/…/Json.scala` lacks them; otherwise use what is there and note it in research.
- [X] T004 [P] Write `MCP/McpServer.scala` from `ANKKA/McpServer.scala`: `Tool`, `ToolResult`, `Resource`, `McpServer` (serve, handleLine, dispatch, the error codes, the supported versions, `startedAtTerminal`, the interactive notice naming `flow mcp`), on the protocol module's `Json`.
- [X] T005 [P] Write `MCP/Docs.scala` from `ANKKA/Docs.scala`: root `ankka-flow/docs/`, uri `ankka-flow://docs/<path>`, `pages`, `read`, `search`.
- [X] T006 Write `MCP/FlowTools.scala`'s shell: the schema helpers, `search_docs`, `read_doc`, `flow_version`, `resources()`, the instructions; `Main.scala`'s `mcp` subcommand serving on stdin/stdout with the notice on stderr. T002 green but for the cluster tools. Commit Phase 2.

---

## Phase 3: User Story 1 — The docs and the pure tools (Priority: P1)

- [X] T007 [US1] Add to `McpServerSuite`: `verify_blueprint` on `CliFixtures.cart` equals `Main.run`'s `verify` output (stdout then stderr), and on a broken variant is an error result with the refusals; `generate_resource` with the cart's images equals `Main.run`'s `generate` YAML; a blueprint path that does not exist is an error result naming it and the session continues (SC-001).
- [X] T008 [US1] In `MCP/FlowTools.scala`, `verify_blueprint` and `generate_resource` through `Verify.run`, `Images`, `ResourceWriter` and `FlowSerialization` — factor `Main.generateResource`'s body into a function returning `Either[Vector[String], String]` that both `Main` and the tool call, so the answer is the command's bytes. Green; commit Phase 3.

**Checkpoint**: an agent can search the docs and verify and generate without a cluster.

---

## Phase 4: User Story 2 — A pipeline on a cluster, read (Priority: P1)

- [X] T009 [US2] Write `MCP/ProjectFile.scala`: `ProjectFile.read(dir): Option[NamedCluster(context, namespace)]` from `flow.toml` (R4's parser), with a `ProjectFileSuite` in `CLIT` for comments, a `[cluster]` header, a missing key and a missing file.
- [X] T010 [US2] Add to `McpServerSuite`: with no `flow.toml`, each of the six cluster tools is an error result saying what to write (the scenario outline); with a `flow.toml` naming a context whose kubeconfig points at the mock server (written as `CliResetSuite` writes it, and passed through the `kubeconfig` property), `list_pipelines` lists two pipelines with their phases; `get_pipeline` shows status, streamlets, topics and events put on the mock; `pipeline_logs` returns the lines a mock expectation serves for the process container and, with `container = sidecar`, the sidecar's; an unknown pipeline is an error result.
- [X] T011 [US2] Write `MCP/Cluster.scala`: `Cluster(named: NamedCluster)` building a fabric8 client from `Config.autoConfigure(context)` with the namespace, closed per call; `pipelines`, `pipeline(name)` with events, `logs(name, streamlet, container, lines)` by the pod labels, `lag(name)` by port-forward and `/metrics` (R5), and `Lag.parse(metrics)` unit-tested in `CLIT/LagSuite.scala` on a captured `/metrics` text. In `FlowTools`, the four read tools with their hints, each refusing without a named cluster. Green; commit Phase 4.

**Checkpoint**: SC-002 holds for the reads.

---

## Phase 5: User Story 3 — A pipeline on a cluster, changed (Priority: P2)

- [X] T012 [US3] Add to `McpServerSuite`: `apply_pipeline` lands the resource on the mock (V2: `serverSideApply`, else `createOrReplace`, named in research) and is refused with nothing sent for a blueprint that does not verify; `reset_pipeline` writes the reset annotation and is refused while a streamlet runs (the cases of `CliResetSuite`, through the tool); both tools' hints say destructive.
- [X] T013 [US3] In `CLI/Reset.scala`, `KubernetesReset` takes its client factory as a parameter (`Reset.kubernetes` keeps today's); in `FlowTools`, `apply_pipeline` and `reset_pipeline`. Green; commit Phase 5.

---

## Phase 6: User Story 4 — Connected without configuration (Priority: P2)

- [X] T014 [P] [US4] Write `MCP/McpInstall.scala` from `ANKKA/McpInstall.scala` with server name `ankka-flow` and command `flow`, and `CLIT/McpInstallSuite.scala` from ankka's: the merge keeping other servers, an existing entry kept unless `--force`, `--dry-run`, Desktop's config path per OS, the scripted fake `claude`; `Main.scala`'s `mcp install`.
- [X] T015 [P] [US4] Write `cli/src/main/templates/common/.mcp.json` and `common/flow.toml` (R4, `{{name}}`), and a "For a coding agent" paragraph in both template READMEs naming the two files and that `flow.toml` is the only cluster the tools touch; `sbt 'cli/testOnly *InitSuite'` green (the index covers them).
- [X] T016 [US4] In `cli/native-smoke.sh`, pipe `initialize`, `resources/list` and a `tools/call` of `verify_blueprint` on `samples/cart-router` into the binary's `flow mcp`; expect `ankka-flow://docs/` resources and `verified:`; run `flow mcp install --scope project --dir` into a temporary directory and check the entry. Run the CLI suite under the tracing agent (`sbt -java-home $GRAALVM_HOME cli/test -Dflow.cli.agent=on -Dflow.template.tests=off`), merge and prune new entries into the metadata (V3), rebuild the binary, `just cli-native` green.
- [X] T017 [US4] Docs: `docs/get-started/coding-agents.md` gains "The MCP server", "Connect Claude to it", "The named cluster" and "What the tools can do" (the tools table, hints, that only `flow.toml`'s cluster is touched); `docs/reference/cli.md` documents `mcp` and `mcp install`; `docs/get-started/first-streamlet.md`'s project table names `.mcp.json` and `flow.toml`; the three skills say what the tools do and which change a cluster. `just docs-sync && just docs` clean. Commit Phase 6.

---

## Phase 7: Polish

- [X] T018 Quickstart tier 3 on kind by hand: `flow init greeter`, the image loaded, Claude Code (or a scripted client) applying, reading status, logs and lag, and resetting; record in `research.md`.
- [X] T019 `CLAUDE.md`: the commands (`flow mcp`, the suites) and a rule: the MCP server touches only `flow.toml`'s cluster, stdout is the protocol, the docs in the binary are the build's. Run the whole build (`caffeinate -i sbt scalafmtCheckAll scalafmtSbtCheck test mutationCheck`, the Python checks, `just features`, `just docs`, the README check, `just cli-native`); tick the reviewer's checklist.
- [X] T020 "Found during implementation" in `research.md` and the pull request description.

---

## Dependencies & Execution Order

- Phase 1 → 2 (the suite needs pages) → 3 → 4 → 5; T003, T004, T005 in parallel after T002.
- Phase 6: T014 and T015 beside Phases 3–5; T016 after Phase 5; T017 last.
- Phase 7 after everything.

## Implementation Strategy

**MVP is US1** (Phases 1–3): the server, the docs and the pure tools, usable with no cluster.
Then the reads, the writes, the connection, polish.
