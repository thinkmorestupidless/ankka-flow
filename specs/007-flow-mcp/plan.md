# Implementation Plan: `flow mcp`

**Branch**: `007-flow-mcp` | **Date**: 2026-10-07 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/007-flow-mcp/spec.md`

## Summary

`flow mcp` ports `ankka mcp`'s hand-written stdio JSON-RPC server, using the protocol module's
`Json` (R1), serves the CLI's own documentation as resources from pages built into the binary
(R2), and offers eleven tools (R3): the pure ones answer exactly what `flow verify`, `flow
generate` and `flow version` print; the cluster ones read and change pipelines through fabric8 on
the one cluster `flow.toml` names (R4, R5), with apply and reset marked destructive. `flow mcp
install` connects Claude Code and Claude Desktop by merging (R6); `flow init` writes `.mcp.json`
and `flow.toml` (R7). The suite drives the server over its protocol against the fabric8 mock
server, the native smoke script initializes the binary's server, and the docs gain the page
sections ankka's have (R8, R9).

## Technical Context

**Language/Version**: Scala 3.9 (the CLI); JSON-RPC 2.0 over stdio; TOML for two keys

**Primary Dependencies**: none added. fabric8 (already the CLI's) for the cluster tools, including
`portForward`; the protocol module's `Json`

**Storage**: `flow.toml` and `.mcp.json` in a project; the docs as classpath resources

**Testing**: `McpServerSuite` and `McpInstallSuite` in `cli/test` (mock API server, a fake
`claude`); `InitSuite`'s index; `cli/native-smoke.sh`; quickstart tier 3 by hand on kind

**Target Platform**: wherever `flow` runs; started by an MCP client over pipes

**Project Type**: a CLI subcommand with two subcommands, templates, documentation

**Performance Goals**: `initialize` and a docs search under a second in the native binary (SC-004)

**Constraints**: stdout is the protocol only; no cluster but the named one; no new library;
warning-free; the native image must carry the docs and the tools' reflection; `flow init`'s suite
still green

**Scale/Scope**: `cli/…/mcp/{McpServer,FlowTools,Docs,Cluster,ProjectFile,McpInstall}.scala`
(about 1,100 lines, half ported), `Main.scala` (+2 subcommands), `Reset.scala` (a client
factory), `build.sbt` (a docs resource generator), 2 template files, 2 suites, the smoke script,
the reachability metadata, 2 docs pages, 3 skills

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

`.specify/memory/constitution.md` is the unfilled template; the gate is `CLAUDE.md`'s rules table.

| Rule | This feature |
|---|---|
| Module direction | Kept: the CLI gains no module edge; it already depends on `protocol`, `blueprint`, `crd`. |
| Pure rendering, one place for I/O | Kept: the tools call `Verify.run`, `ResourceWriter.write` and the fabric8 paths the commands use; nothing new renders. |
| The resource says what runs | Kept: `apply_pipeline` applies what `generate_resource` writes. |
| Kafka credentials reach only the sidecar | Untouched; lag is read from the sidecar's metrics, not from Kafka. |
| No literal image tags in tests | Kept. |
| Warning-free compile | Kept. |
| Test switches must be forwarded | No new switch. |
| Not in this build | Nothing added: the JSON is the protocol module's, the HTTP for lag is the JDK's. |
| Docs: a page stands alone; new pages in nav and a skill | Kept (R9); sections, no new page. |
| `flow init`'s templates are plain files | Kept: two files added to `common/`. |

**Violations to justify**: none.

## Project Structure

### Documentation (this feature)

```text
specs/007-flow-mcp/
├── plan.md, research.md, data-model.md, quickstart.md
├── contracts/mcp-tools.md
├── checklists/requirements.md
└── tasks.md
```

Scenarios: `features/cli/mcp.feature`, one in `features/cli/init.feature`.

### Source Code (repository root)

```text
cli/src/main/scala/com/thinkmorestupidless/ankka/flow/cli/
├── mcp/McpServer.scala      # the protocol (ported)
├── mcp/Docs.scala           # pages, search, resources (ported)
├── mcp/ProjectFile.scala    # flow.toml
├── mcp/Cluster.scala        # a client for the named cluster; list, get, events, logs, lag, apply
├── mcp/FlowTools.scala      # the eleven tools and the instructions
├── mcp/McpInstall.scala     # install (ported)
├── Reset.scala              # KubernetesReset takes a client factory
└── Main.scala               # `mcp`, `mcp install`
cli/src/main/templates/common/{.mcp.json,flow.toml}; the two READMEs
cli/src/main/resources/META-INF/native-image/…/reachability-metadata.json   # ankka-flow/docs/**, new reflection
cli/src/test/scala/…/cli/{McpServerSuite,McpInstallSuite}.scala; InitSuite
cli/native-smoke.sh
build.sbt                    # the docs resource generator
docs/get-started/coding-agents.md, docs/reference/cli.md, docs/get-started/first-streamlet.md (two files named), tools/docs/skill/*
```

**Structure Decision**: a package `mcp` inside the CLI, as ankka's; no new module.

## Order of work

1. `McpServer` on the protocol module's `Json`, `Docs` and the docs resource generator; the
   pure tools; `McpServerSuite`'s protocol and pure cases green (tier 1).
2. `ProjectFile`, `Cluster`, the six cluster tools, `Reset`'s factory; the mock-server cases.
3. `McpInstall` and its suite; `flow init`'s two files; the READMEs.
4. The native image: docs glob, the agent run for new reflection, the smoke script.
5. The docs and skills; tier 3 by hand on kind.

## Complexity Tracking

| Addition | Why Needed | Simpler Alternative Rejected Because |
|---|---|---|
| A hand-written JSON-RPC server (~250 lines) | four methods over stdio (R1) | an MCP SDK is a dependency with its own JSON and threads |
| A two-key TOML reader | the named cluster lives in `flow.toml` (R4, clarified) | a TOML library for two keys |
| A port-forward per `pipeline_lag` call | lag lives on the sidecar's port (R5, clarified) | asking the person to port-forward by hand, which is what the tool replaces |

## Constitution Check (post-design)

Unchanged: no module edge, no library, no switch. Found while planning: `FlowClusterSuite` already
port-forwards 2050 through fabric8, so the lag path has a proven call; the fabric8 mock cannot
port-forward, so lag's parser is unit-tested and the whole path is tier 3 and the k3s suite.

## Not in this feature

Prompts, sampling, HTTP transports; platform installation tools; deleting a pipeline; any cluster
but the named one; changes to the SDKs, the sidecar, the operator or the protocol.
