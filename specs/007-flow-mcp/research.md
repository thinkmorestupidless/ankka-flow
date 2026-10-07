# Research: `flow mcp`

## R1. ankka's server, with the protocol module's JSON

**What is there**: `ankka mcp` is a hand-written JSON-RPC 2.0 server over stdio (`McpServer.scala`,
248 lines): `initialize` negotiating among four protocol versions, `ping`, `tools/list`,
`tools/call`, `resources/list`, `resources/read`, an empty `resources/templates/list`; notifications
ignored, batches tolerated; a tool's failure is an `isError` result, a protocol failure a JSON-RPC
error; a notice on stderr when started at a terminal. It carries its own 118-line JSON AST.

**Decision**: port the server as it is, under `cli/…/mcp/`, and use the protocol module's `Json`
(AST, parser, compact printer) instead of a second one: `Json.Obj` fields, `Json.parse`,
`Json.compact`. A `field`/`string` accessor pair is added where ankka's AST had them. No MCP
library and no JSON library enter the build.

**Alternatives considered**: an MCP SDK for Java — a dependency with its own JSON and threading
for four methods. Copying ankka's `Json.scala` — two JSON ASTs in one CLI.

## R2. The documentation inside the binary

**Decision**: a `Compile / resourceGenerators` task on `cli` copies every `docs/**/*.md` to
`ankka-flow/docs/<path>` with an `index.txt`, as the templates are (feature 006, R1), and the
native image registers `ankka-flow/docs/**`. `Docs` reads a page's `title`, `description` and
`kind` from its frontmatter; `search_docs` scores a query's words over title, description and
body (ankka's scorer); `read_doc` returns the page; every page is a resource at
`ankka-flow://docs/<path>` of type `text/markdown`. The pages served are the ones this `flow` was
built from, so the docs always match the CLI.

## R3. The tools

| Tool | Hints | Does |
|---|---|---|
| `verify_blueprint` | read-only | `Verify.run` on a blueprint path, a descriptors directory and `--conf` files; the notes, the summary line, or the refusals |
| `generate_resource` | read-only | `Verify.run` then `ResourceWriter.write` with images, pipeline id, version and namespace; the YAML `flow generate` prints; nothing applied |
| `flow_version` | read-only | `flow version`'s line |
| `search_docs` | read-only | pages ranked for a query |
| `read_doc` | read-only | one page as Markdown |
| `list_pipelines` | read-only, cluster | every `AnkkaFlow` in the named namespace: name, phase, detail, streamlets' ready/desired |
| `get_pipeline` | read-only, cluster | one pipeline's spec summary, status (phase, detail, streamlets, topics) and the last events on it (`involvedObject.kind=AnkkaFlow`, name) plus `PartitionStalled` events on its pods |
| `pipeline_logs` | read-only, cluster | the last N lines (default 200) of one streamlet's `process` or `sidecar` container, by the pod labels the sidecar's reset already uses |
| `pipeline_lag` | read-only, cluster | per streamlet pod: port-forward 2050, GET `/metrics`, the `records_lag` lines parsed to (client id, topic, partition, lag) |
| `apply_pipeline` | destructive, cluster | `generate_resource`'s inputs; verifies, writes the resource, server-side applies it in the named namespace; refuses a blueprint that does not verify before anything is sent |
| `reset_pipeline` | destructive, cluster | `KubernetesReset.request` with the named cluster's client: the same guards and the same annotation |

Every tool's text answer is what the command prints, so SC-001 is a byte comparison in the suite.

## R4. The named cluster: `flow.toml`

**Decision** (clarified): `flow mcp` reads `flow.toml` from the directory it is started in:

```toml
# The cluster flow mcp's tools may touch. Nothing else is ever used, whatever kubectl points at.
context   = "kind-ankka"
namespace = "greeter"
```

Two keys, read by a small parser of `key = "value"` lines (comments and blank lines ignored; an
optional `[cluster]` table header accepted); a TOML library would be a dependency for two keys.
`flow init` writes it with `kind-ankka` and the project's name. With no file, or no `context`, every
cluster tool answers `no cluster named: write context and namespace in flow.toml …` as an error
result; the server says on stderr at start whether it has a cluster. The kubeconfig's current
context is never consulted.

## R5. Reaching the cluster

**Decision**: one `KubernetesClient` per tool call, built from
`Config.autoConfigure(context)` with the namespace set, and closed after — as `KubernetesReset`
builds and closes one per request. `KubernetesReset` gains a client factory parameter so the MCP
tool passes the named cluster's. Apply is `client.resource(flow).inNamespace(ns).serverSideApply()`
with a field manager `flow`, which is what `kubectl apply --server-side` does. Logs:
`pods.withLabel(pipeline).withLabel(streamlet)` then `.inContainer(name).tailingLines(n).getLog`.
Events: `client.v1().events().inNamespace(ns).withField("involvedObject.name", name)`. Lag:
`pod.portForward(2050)` — the call `FlowClusterSuite` already makes against k3s — then an HTTP GET
of `/metrics` on the local port, with the `records_lag` lines parsed.

## R6. `flow mcp install`

**Decision**: port `McpInstall`: `--scope user` (default) runs `claude mcp add --scope user
ankka-flow -- flow mcp`; `--scope project` merges `{"ankka-flow": {"command": "flow", "args":
["mcp"]}}` into `.mcp.json` (`--dir`); `--client desktop` merges into Claude Desktop's config with
the absolute path of the first `flow` on `PATH` (and `JAVA_HOME` for the JVM build); every write
merges, an existing `ankka-flow` entry is kept unless `--force`, `--dry-run` prints the change.
The server's name in every client is `ankka-flow`.

## R7. `flow init` writes the connection

**Decision**: `cli/src/main/templates/common/.mcp.json` (`flow mcp`, by name) and
`common/flow.toml` (tokens `{{name}}`); the template READMEs gain a paragraph; `InitSuite`'s index
check covers both.

## R8. The proof

- `McpServerSuite`: the protocol against the server with a fabric8 `KubernetesMockServer` (CRUD
  dispatcher, as `CliResetSuite`) reached through a temporary kubeconfig naming a context — version
  negotiation; a parse error; notifications ignored; every tool listed with schema and hints;
  `verify_blueprint` and `generate_resource` byte-equal to `Main.run`'s output on the test
  blueprints; `flow_version`; `search_docs` finds a page by title and `read_doc` returns it; every
  page a resource; the six cluster tools refused with no `flow.toml`; `list_pipelines`,
  `get_pipeline` (status and events put on the mock), `pipeline_logs` (a mock expectation for the
  log endpoint), `apply_pipeline` (the resource lands on the mock; an unverifiable blueprint sends
  nothing), `reset_pipeline` (the annotation, and the running-streamlet refusal) against the mock.
- `pipeline_lag`: the metrics parser unit-tested on a captured `/metrics`; the port-forward path is
  the one `FlowClusterSuite` proves against k3s, and quickstart tier 3 runs the tool on kind by hand.
- `McpInstallSuite`: the merge rules, Desktop's paths, a scripted fake `claude`, `--dry-run`.
- `native-smoke.sh`: pipe `initialize`, `resources/list` and a `verify_blueprint` call into the
  binary's `flow mcp`; expect `ankka-flow://docs/` resources and `verified:` in the answer.
- Fabric8's port-forward and events paths may reach classes the image's metadata does not list:
  the suite runs under the tracing agent once (`-Dflow.cli.agent=on`), the new entries are merged
  and pruned, and the smoke script's tool call holds them.

## R9. Documentation

**Decision**: `docs/get-started/coding-agents.md` gains "The MCP server", "Connect Claude to it",
"What the tools can do" and "The named cluster" on ankka's page's shape; `reference/cli.md`
documents `mcp` and `mcp install`; the skills say what the tools are for and which change a
cluster; `flow init`'s page section names the two files.

## Verify first

1. **R1**: the protocol module's `Json` round-trips every message shape the server needs
   (nested objects, arrays, numbers for ids, escaped strings); ankka's clients send ids as numbers
   and strings.
2. **R5**: the fabric8 mock server honours `serverSideApply` on a custom resource with the CRUD
   dispatcher; if not, `createOrReplace` in the suite with the real call named.
3. **R8**: which reachability entries the agent adds for port-forward and events.
4. **R4**: a `flow.toml` in a project written by `flow init` is picked up by `flow mcp` started by
   Claude Code, whose working directory is the project's root.
