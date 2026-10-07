# Data Model: `flow mcp`

| Entity | Fields | Rules |
|---|---|---|
| server | name `ankka-flow`, version, instructions, tools, resources, named cluster (optional) | serves one client over stdio until input ends; stdout is the protocol only |
| tool | name, title, description, input schema, hints (readOnly, destructive, idempotent, openWorld), run | a failure is an error result; a cluster tool refuses without a named cluster |
| resource | uri `ankka-flow://docs/<path>`, name, title, description, `text/markdown` | one per page in the CLI's `index.txt` |
| named cluster | context, namespace | from `flow.toml` in the start directory; both required for any cluster tool |
| project file | `flow.toml`: `context`, `namespace` | written by `flow init`; read, never written, by `flow mcp` |
| connection file | `.mcp.json` (`{"mcpServers": {"ankka-flow": {"command": "flow", "args": ["mcp"]}}}`), or a client's settings | merged, never replaced; the project's names the command, Desktop's an absolute path |
| lag reading | streamlet, pod, client id, topic, partition, lag | parsed from `records_lag` lines of a pod's `/metrics` |

States of a session: **waiting** → **initialized** (version agreed) → **serving** (calls answered) →
**ended** (input closed). A tool call: **received** → **answered** (text, or text marked error).
