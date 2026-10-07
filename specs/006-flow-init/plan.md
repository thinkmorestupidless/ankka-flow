# Implementation Plan: `flow init`

**Branch**: `006-flow-init` | **Date**: 2026-10-07 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/006-flow-init/spec.md`

## Summary

`flow init <name> --language scala|python` renders a streamlet project from templates the CLI
carries as resources, ankka's embedded way: an index per language, exact `{{token}}` replacement,
the ankka-flow plugin's skills copied in at build time (R1, R2). The project depends on the SDK and
the sidecar image at the CLI's own version, and its committed descriptor names the SDK version the
published SDK of that version reports (R3, R4). The streamlet adds a `greeting` to each JSON
object; the README's laptop loop uses Kafka's console tools (R5). A unit suite holds the refusals
and the rendering; a template suite builds each language's project against this repository's SDK
and runs its tests, descriptor check, image build and `flow verify`; the native smoke script writes
both and byte-compares them with the JVM build (R6). The tutorial starts from `flow init` (R7).

## Technical Context

**Language/Version**: Scala 3.9 (the CLI); the templates: Scala 3.3 LTS with sbt, Python 3.12 with
uv; YAML for the generated workflow

**Primary Dependencies**: nothing new in the CLI (decline is there). The templates use the
published SDKs and sbt-native-packager

**Storage**: files: templates as classpath resources; the project on disk

**Testing**: munit in `cli/test`: `InitSuite` always; `TemplateSuite` gated by
`-Dflow.template.tests` (forwarded); `cli/native-smoke.sh`; quickstart tier 3 by hand

**Target Platform**: wherever `flow` runs, the native binary included

**Project Type**: a CLI command, its templates, documentation

**Performance Goals**: `flow init` under a second with no network (SC-004)

**Constraints**: no change to the SDKs, sidecar, operator or protocol; module direction kept (the
CLI gains no dependency); every new `-Dflow.*` switch forwarded; warning-free compile; the native
binary must carry the templates

**Scale/Scope**: `Init.scala` and `Scaffold.scala` in the CLI, `Main.scala` (+1 subcommand);
`build.sbt` (a resource generator, a switch, the template suite's `publishLocal` dependency);
two templates and a shared directory (about 15 files each, plus the skills); 2 suites; the smoke
script; the reachability metadata; CI's path filter; 4 docs pages and 2 skills

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

`.specify/memory/constitution.md` is the unfilled template; the gate is `CLAUDE.md`'s rules table.

| Rule | This feature |
|---|---|
| Module direction | Kept: the CLI gains no module dependency; templates are its resources. |
| Explicit registration | Kept: the generated streamlet declares its ports with the SDK's factories. |
| No literal image tags in tests | The template's compose file names the sidecar by `{{flow_version}}`; the template suite builds images by the CLI's version. |
| Warning-free compile | Kept. |
| Test switches must be forwarded | `flow.template.tests` is added to `forwardedTestSwitches` in the commit that reads it. |
| Docs: a page stands alone; new pages in nav and a skill | Kept (R7); no new page is needed beyond sections. |
| Samples are included from tested code | The tutorial's snippets of the generated project come from the templates, which the template suite builds. |
| Everything else (sidecar, Kafka, commit order, rendering, credentials, built-ins, "not in this build") | Untouched. |

**Violations to justify**: none.

## Project Structure

### Documentation (this feature)

```text
specs/006-flow-init/
├── plan.md, research.md, data-model.md, quickstart.md
├── contracts/flow-init.md
├── checklists/requirements.md
└── tasks.md             # /speckit-tasks output
```

The scenarios are in `features/cli/init.feature` and `features/cli/install-page.feature`.

### Source Code (repository root)

```text
cli/src/main/scala/com/thinkmorestupidless/ankka/flow/cli/
├── Init.scala            # the request, its refusals, the tokens
├── Scaffold.scala        # reads the index, renders paths and contents, writes the project
└── Main.scala            # the `init` subcommand
cli/src/main/templates/
├── common/               # blueprint.conf, flow/streamlet.conf, docker-compose.yml, k8s/in-cluster.conf, .gitignore
├── scala/                # build.sbt, project/*, src/**, flow/descriptor.json, README.md, .github/workflows/ci.yml
└── python/               # pyproject.toml, src/**, tests/**, Dockerfile, .dockerignore, flow/descriptor.json, README.md, .github/workflows/ci.yml
cli/src/main/resources/META-INF/native-image/…/reachability-metadata.json   # + ankka-flow/templates/**
cli/src/test/scala/…/cli/{InitSuite,TemplateSuite}.scala
cli/native-smoke.sh       # + init for both languages, cmp -r against the JVM build
build.sbt                 # the resource generator; flow.template.tests; TemplateSuite depends on sdk/protocol publishLocal
.github/workflows/ci.yml  # the cli filter covers cli/src/main/templates/** and the plugin's skills
docs/get-started/{first-streamlet,install}.md, docs/reference/cli.md, docs/contributing/building.md, tools/docs/skill/*
```

**Structure Decision**: no new module. `init` is a command of the CLI; its templates are the CLI's
resources, built from plain files in `cli/src/main/templates/`.

## Order of work

1. `Init` and `Scaffold` with the refusals and the token rendering, against a two-file test
   template; `InitSuite` green (tier 1).
2. The resource generator with the index, hidden files and the skills; V1 in the jar.
3. The Scala template; `TemplateSuite` for Scala green (V2).
4. The Python template; `TemplateSuite` for Python green (V3).
5. The native image: the resource glob, the smoke script's init and `cmp -r` (tier 4).
6. CI's filter; the docs and skills; the laptop loop by hand (tier 3).

## Complexity Tracking

| Addition | Why Needed | Simpler Alternative Rejected Because |
|---|---|---|
| A resource generator and an index | a native image cannot list a resource directory (R1) | Giter8 needs sbt and the network and is not version-pinned |
| A template suite that builds two projects | a template that renders but does not build is the failure that matters (R6) | rendering checks alone pass a broken `build.sbt` |
| The descriptor as a token-filled template file | the project must commit it and the CLI must not hold a second declaration (R4) | writing it from a CLI-held declaration duplicates the streamlet |

## Constitution Check (post-design)

Unchanged: no module edge, no library, one forwarded switch, no change outside the CLI and the
docs.

Found while planning:

- A streamlet name may start with a digit, but a project name may not: it becomes a class, a
  package and a module. `flow init` refuses one (R2); the descriptor rules are unchanged.
- The committed descriptor names the SDK version by the SDKs' own rule, the release's version or
  `0.0.0` (R3), so a project written by a `flow` built from source still passes its check against
  this repository's SDK.

## Not in this feature

`flow mcp` and a `.mcp.json`; more languages; multi-streamlet pipelines; templates fetched from
elsewhere; changes to the SDKs, sidecar, operator or protocol.
