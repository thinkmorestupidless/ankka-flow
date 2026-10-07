# Contract: `flow mcp`, its tools and resources

```text
flow mcp                                   # serve on stdin/stdout; reads ./flow.toml for the named cluster
flow mcp install [--scope user|project] [--client code|desktop] [--dir <project>] [--command <path>] [--force] [--dry-run]
```

Server name `ankka-flow`; protocol versions `2025-11-25`, `2025-06-18`, `2025-03-26`, `2024-11-05`
(newest offered wins); capabilities `tools` and `resources`; `instructions` naming the named cluster
or its absence.

## Tools

| Tool | Arguments | Hints | Answer |
|---|---|---|---|
| `verify_blueprint` | `blueprint` (path), `descriptors` (dir, optional), `conf` (paths, optional) | read-only | `flow verify`'s stdout and stderr, in that order; error when refused |
| `generate_resource` | as above plus `images` (map name→image), `pipeline`, `version`, `namespace`, `delete_managed_topics` | read-only | the resource YAML; error when refused |
| `flow_version` | — | read-only | `flow <version>, protocol <protocol>` |
| `search_docs` | `query`, `limit` (default 5) | read-only | `path — title: description` per page |
| `read_doc` | `path` | read-only | the page's Markdown |
| `list_pipelines` | — | read-only, open-world | `name  phase  detail  streamlets ready/desired` per pipeline in the named namespace |
| `get_pipeline` | `name` | read-only, open-world | the spec's streamlets and images, the status, the last 20 events on it and `PartitionStalled` events on its pods |
| `pipeline_logs` | `name`, `streamlet`, `container` (`process` default or `sidecar`), `lines` (default 200) | read-only, open-world | the lines |
| `pipeline_lag` | `name` | read-only, open-world | `streamlet  pod  client_id  topic  partition  lag` per inlet partition |
| `apply_pipeline` | `generate_resource`'s arguments | destructive, idempotent, open-world | `applied <name> in <namespace>`; error when the blueprint does not verify (nothing sent) |
| `reset_pipeline` | `name`, `streamlets` (list, optional) | destructive, open-world | `reset requested for '<name>': <id>`; `flow reset`'s refusals as errors |

A cluster tool with no named cluster answers, as an error:
`no cluster named: write context = "<kubectl context>" and namespace = "<namespace>" in flow.toml beside the project`.

## Resources

`ankka-flow://docs/<path>` for every page in `docs/`, `text/markdown`, named by the page's title.

## `flow.toml`

```toml
context   = "kind-ankka"
namespace = "greeter"
```

Both keys strings; comments with `#`; a `[cluster]` header tolerated. Any other key is ignored.
