---
title: Adding a language SDK
description: What an SDK for another language must implement, how it copies the protocol, and how the descriptor fixtures and the conformance suite prove it compatible.
kind: contributing
related: [reference/protocol.md, reference/descriptor.md, reference/python-sdk.md]
---

# Adding a language SDK

Any language can host a streamlet: the process speaks the [streamlet protocol](../reference/protocol.md)
to the sidecar in its pod, and the sidecar does everything else. An SDK is compatible when it passes two
things, the **descriptor fixtures** and the **conformance suite**. Nothing else about it is prescribed.
The Python SDK in
[`sdks/python`](https://github.com/thinkmorestupidless/ankka-flow/blob/main/sdks/python) is the worked
example; its [reference](../reference/python-sdk.md) shows one shape an SDK can take.

## What an SDK implements

1. **Declaration.** A way to declare a streamlet: its name, its inlets and outlets with JSON contracts
   (a schema name, fingerprinted as the Base64 of the SHA-256 of that name), and typed parameters of the
   six `ConfigType`s. Registration is explicit; the SDK never discovers streamlets by scanning.
2. **The descriptor writer.** The declaration as canonical JSON, exactly as the
   [Descriptor](../reference/descriptor.md) page defines it: snake_case field names, sorted keys, ports
   sorted by name, defaults omitted, two-space indentation, one trailing newline. It should also refuse
   what the validation rules refuse.
3. **Two gRPC services**, served on `127.0.0.1:$FLOW_PROCESS_PORT` and nowhere else: `Discovery`
   (answer `Discover` with the descriptor; log the problems `ReportError` carries) and `Streamlet.Run`.
4. **The conversation.** Wait for `Start` and apply its `config_json`. For each `Batch`, call the user's
   code, send each `Emit` as it is produced, then exactly one `Ack`, or one `Fail` if the code raised.
   Run batches of different partitions concurrently; never reorder one batch's messages. A new `Run`
   voids everything tied to the previous one. On `Stop`, finish in-flight batches and complete the
   stream. An emit without a key leaves `Record.key` unset.

Everything else — codecs, test harnesses, project templates, the build tool — is the SDK's own
business.

## Copying the protocol

Copy the whole `protocol/` directory into the SDK verbatim: the `.proto` files, `README.md`,
`DESCRIPTOR.md` and `fixtures/`. Generate code from the copy. CI diffs every copy against `protocol/`, so
a change to the protocol is a change to every SDK in the same commit. The Python SDK's
`scripts/proto.py` does the copy and the generation.

## The descriptor fixtures

`protocol/fixtures/declarations/*.md` describe six streamlets in prose. Declare each in the SDK's
language, write its descriptor with the `sdk` block pinned to `{"name": "fixture", "version": "0.0.0"}`,
and assert that the bytes equal `protocol/fixtures/descriptors/<name>.json`.

## The conformance suite

Implement the reference streamlet of `protocol/fixtures/declarations/conformance.md`. It behaves by each
record's key: `echo` emits the record to `out`; `fan` emits it to `out` and `other`; `skip` emits
nothing; `fail` fails the batch; `late` waits 300 ms, then echoes; `rogue-outlet` emits to an outlet
named `nope`; `multiply` emits the record `factor` times with a header `n=<i>`; `unkeyed` emits it with
no key; `header-echo` emits it with its headers reversed; any other key echoes.

Serve it on a port and run the suite from the ankka-flow repository:

```bash
sbt 'sidecar/testOnly *ConformanceSuite' -Dflow.conformance.target=127.0.0.1:9010
sbt 'sidecar/testOnly *ConformanceSuite' -Dflow.conformance.target=127.0.0.1:9010 -Dflow.conformance.only=run.fan-out
```

Every case is named for the conversation it checks, such as `run.emits-precede-ack` or
`run.two-partitions-interleave`, so a failure says what the SDK got wrong. The scripted inputs each case
sends are in `protocol/fixtures/conversations/`, readable without Scala. The Python SDK wraps this in
`uv run conformance`; a new SDK can offer the same.

The `violation.*` and `version.*` cases are skipped against a process. They prove that the sidecar
refuses misbehaviour a correct SDK cannot produce — a double acknowledgement, an emit after its ack, an
unknown batch, another major version — using a scriptable double inside the suite's own JVM.
