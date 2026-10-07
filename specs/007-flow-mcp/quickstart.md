# Quickstart: `flow mcp`

## Tier 1 — the protocol and the pure tools

```bash
sbt 'cli/testOnly *McpServerSuite *McpInstallSuite'
printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list"}' | cli/target/universal/stage/bin/flow mcp
```

Expected: the suites green; eleven tools listed with hints; `ankka-flow://docs/` resources.

## Tier 2 — the cluster tools against the double

Covered by `McpServerSuite`: list, get, logs, apply and reset against the fabric8 mock server; every
cluster tool refused with no `flow.toml`.

## Tier 3 — on kind, by hand

```bash
flow init greeter && cd greeter && sbt Docker/publishLocal && kind load docker-image --name ankka greeter:0.1.0
claude        # in the project: Claude Code asks to trust .mcp.json, then has the tools
```

Ask the agent to apply the pipeline, read its status, its logs and its lag, and reset it; each
tool that changes the cluster is confirmed first. Expected: `apply_pipeline` makes the pipeline
Ready once `greeter.in` exists; `pipeline_lag` lists `greeter.greeter.in`'s partitions.

## Tier 4 — the native binary

`just cli-native`: the smoke script initializes the binary's server, lists its resources and
verifies a sample through the tool.

## Reviewer's checklist

- [ ] stdout carries only the protocol; the terminal notice is on stderr
- [ ] every tool has a description, a schema and hints; apply and reset are destructive
- [ ] no cluster tool reads the kubeconfig's current context; all read `flow.toml`
- [ ] pure tools' answers are byte-equal to the commands' output
- [ ] every docs page is a resource; the docs in the binary are this build's
- [ ] `flow init` writes `.mcp.json` and `flow.toml`; `InitSuite`'s index covers them
- [ ] `install` merges and never overwrites another server
- [ ] the native smoke script exercises the server
- [ ] nothing in the SDKs, the sidecar, the operator or the protocol changed
