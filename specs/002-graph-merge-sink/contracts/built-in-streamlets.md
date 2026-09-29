# Contract: built-in streamlets

What it means, to each tool, for a blueprint to name a descriptor the platform ships rather than one
an SDK wrote. The first and only built-in in this feature is `neo4j-merge-sink`
([neo4j-merge-sink.md](./neo4j-merge-sink.md)); this file is the mechanism.

## Blueprint

`streamlets { <name> = builtin/<built-in name> }`. The `builtin/` prefix is recognised at lookup:
a built-in descriptor matches only its prefixed name, and a descriptor file matches only its bare
name, so neither can shadow the other. Everything else about the streamlet — ports in topics,
`--conf` parameters and `replicas` — is as for any streamlet.

## `flow verify`

- Knows `Builtins.all` without any file; `--descriptors` is optional, and a blueprint of built-ins
  alone verifies.
- A built-in's inlet is verified against its topic's producers as any port; an inlet connected to
  nothing, an outlet it does not have, or a parameter it does not declare is refused with the
  existing messages.
- New problem: `Streamlet '<name>' names built-in descriptor 'builtin/<x>', which this version does
  not have; the built-ins are: neo4j-merge-sink.`

## `flow generate`

- Needs no image for a built-in streamlet: the missing-image refusal skips it.
- Refuses an image given for one: `Streamlet '<name>' is built in and takes no image.`
- Writes `builtin: true`, `image: ""`, the built-in descriptor embedded, `config` and `replicas` as
  resolved.

## The resource

`spec.streamlets[]` gains `builtin: boolean` (default `false`). `image` is no longer required by
the schema; it is required for a streamlet that is not built in, which the operator checks. The
CRD's schema and the case class change together (`CrdSchemaSuite`).

## The operator

For a streamlet with `builtin: true`:

- **Refuses** (`Refused` event, phase `Failed`, no other action): an `image` that is not empty
  (`streamlet '<name>' is built in and takes no image`); a built-in name the operator does not know
  (`streamlet '<name>' names built-in 'x', which this operator does not know`); the stage's Secret
  missing or incomplete (neo4j-merge-sink.md).
- **Renders** the Deployment with one container, `sidecar`, and no `FLOW_PROCESS_ADDRESS`; the
  config Secret as for any streamlet, with the `stage` block in `streamlet.conf`; the stage's
  Secret as a read-only volume; the config hash covering the stage block and the stage Secret's
  `resourceVersion`.
- **Observes** roll-out and settling as for any streamlet; the process image is `""` on both sides.
- Everything else — topics, the pipeline's ServiceAccount and Role, events, reset, status — is
  unchanged.

## The sidecar

`streamlet.conf` with a `flow.stage { name = <built-in name>, … }` block selects stage mode:

- No process: `FLOW_PROCESS_ADDRESS` is not read, no channel, no discovery, no `Start`/`Stop`.
- The deployed `descriptor.json` must equal the image's built-in of that name (exit 1 otherwise);
  `stage.name` must be one the image has (exit 2 otherwise).
- Readiness: every inlet subscribed and the stage open.
- Failure, backoff, redelivery, stalls, metrics and events: as for a process, with the stage's
  `open()` where discovery was.

## Compatibility

A resource written by a CLI of one version and run by an operator and sidecar of another: the
operator runs any built-in name it knows; the sidecar refuses a descriptor that differs from its
own (a parameter added or removed between versions), so the mismatch is visible as a failed pod
with the differences in its log, never as a silently ignored parameter.
