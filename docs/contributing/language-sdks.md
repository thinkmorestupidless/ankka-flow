# Writing an SDK for another language

An SDK is compatible when it passes two things: the **descriptor fixtures** and the
**conformance suite**. Nothing else about it is prescribed. The Python SDK in `sdks/python` is the
worked example.

## What an SDK implements

1. **Declaration.** A way to declare a streamlet: its name, inlets and outlets with JSON contracts
   (a schema name; the fingerprint is Base64 of SHA-256 of that name), and typed parameters.
2. **The descriptor writer.** The declaration as canonical JSON, exactly as `protocol/DESCRIPTOR.md`
   defines: snake_case field names, sorted keys, ports sorted by name, defaults omitted, two-space
   indent, one trailing newline.
3. **Two gRPC services**, served on `127.0.0.1:$FLOW_PROCESS_PORT` and nowhere else:
   `Discovery` (answer `Discover` with the descriptor; log `ReportError`'s problems) and
   `Streamlet.Run` (see below).
4. **The conversation.** Wait for `Start` and apply its `config_json`. For each `Batch`, call the
   user's code, send each `Emit` as it is produced, then exactly one `Ack`, or a `Fail` if the code
   raised. Run batches of different partitions concurrently; never reorder one batch's messages.
   A new `Run` voids everything tied to the previous one. On `Stop`, finish and complete the stream.

Everything else, such as codecs, test harnesses and project templates, is the SDK's own business.

## Copying the protocol

Copy the whole `protocol/` directory into the SDK verbatim: the `.proto` files, `README.md`,
`DESCRIPTOR.md` and `fixtures/`. Generate code from the copy. CI diffs every copy against
`protocol/`, so a change to the protocol is a change to every SDK in the same commit.

## The descriptor fixtures

`protocol/fixtures/declarations/*.md` describe six streamlets in prose. Declare each in your
language, write its descriptor with the `sdk` block pinned to `{"name": "fixture", "version": "0.0.0"}`,
and assert the bytes equal `protocol/fixtures/descriptors/<name>.json`.

## The conformance suite

Implement the reference streamlet of `fixtures/declarations/conformance.md`, behaving by each
record's key (`echo`, `fan`, `skip`, `fail`, `late`, `rogue-outlet`, `multiply`, `unkeyed`,
`header-echo`, anything else echoes). Serve it on a port and run, from the ankka-flow repository:

```bash
sbt 'sidecar/testOnly *ConformanceSuite' -Dflow.conformance.target=127.0.0.1:9010
sbt 'sidecar/testOnly *ConformanceSuite' -Dflow.conformance.target=127.0.0.1:9010 -Dflow.conformance.only=run.fan-out
```

Every case is named for the conversation it checks (`run.emits-precede-ack`,
`run.two-partitions-interleave`), so a failure says what your SDK got wrong. The scripted inputs
each case sends are in `protocol/fixtures/conversations/`, readable without Scala.

The `violation.*` and `version.*` cases are skipped against a process: they prove the sidecar
refuses misbehaviour that a correct SDK cannot produce, using a scriptable Scala double.
