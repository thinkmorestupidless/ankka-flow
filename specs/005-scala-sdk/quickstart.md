# Quickstart: A Scala SDK

How to see that the feature works, in the order the risk falls. Tiers 1–3 run on a laptop with
Docker, a JDK 21 and sbt; tier 4 needs the secrets and a tag.

## Tier 1 — the SDK, with no Kafka and no sidecar

```bash
sbt protocol/test sdk/test
```

Expected: the six fixture descriptors reproduced byte for byte; the harness suite (routing,
partitions and batch sizes, a failing batch, an undeclared outlet, a parameter's value and default,
bytes unchanged and nothing held); `Descriptor` writing and `--check`ing a file; a refused
declaration refused at construction. `protocol` compiles on the LTS Scala with no warning, and so
does everything that depends on it.

## Tier 2 — the protocol

```bash
sbt 'sidecar/testOnly *ConformanceSuite'          # the in-process target is now the Scala SDK
sbt 'sdk/Test/runMain com.thinkmorestupidless.ankka.flow.sdk.conformance.ConformanceMain 9010' &
sbt 'sidecar/testOnly *ConformanceSuite' -Dflow.conformance.target=127.0.0.1:9010
```

Expected: every `run.*` case passes in both modes; the `violation.*` and `version.*` cases run
against the double in the first and are skipped in the second, as for the Python SDK.

## Tier 3 — the sample, the laptop loop, the docs, the README

```bash
sbt cartRouterScala/test cartRouterScala/descriptorCheck cartRouterScala/docker:publishLocal
diff <(jq .streamlet samples/cart-router-scala/flow/descriptor.json) <(jq .streamlet samples/cart-router/flow/descriptor.json)
flow verify samples/cart-router/blueprint.conf --descriptors samples/cart-router-scala/flow
sbt sidecar/docker:publishLocal
(cd samples/cart-router && docker compose up -d)
sbt cartRouterScala/run &                          # the Scala router on 127.0.0.1:9010
(cd samples/cart-router && uv run python produce.py && uv run python verify.py)
just docs && just readme-sync && git diff --exit-code README.md && just features
```

Expected: the two descriptors' `streamlet` parts are identical; `flow verify` prints the same
result as against the Python descriptors; the sidecar discovers the Scala router and the round
trip verifies; the docs build is clean with the Scala tabs and the two new pages; the README's
Scala block is current.

## Tier 4 — the release

With the four secrets set, tag `v0.5.0-rc.1` on the branch and observe that `sdk-scala`, `images`,
`sdk-python` and `marketplace` are skipped (a pre-release publishes none of it) and the binaries'
jobs run as before. The real proof is the first release tag: `ankka-flow-sdk_3` and
`ankka-flow-protocol_3` on Maven Central at the release's version, resolvable by a fresh sbt
project that compiles the cart router and runs its tests; `sample-cart-router-scala` in the
registry. Before that, the local rehearsal:

```bash
sbt -Dflow.release.local=/tmp/m2 publishSigned      # with a throwaway gpg key
ls /tmp/m2/com/thinkmorestupidless/ankka-flow-sdk_3/
```

and a fresh project with `resolvers += "local-release" at "file:///tmp/m2"` compiling the router.

## Reviewer's checklist

- [ ] no file under `protocol/` changed; no Python SDK behaviour changed; the sidecar's suites pass as before
- [ ] `protocol`, `sdk` and the sample are on the LTS Scala; everything else on 3.9; warning-free
- [ ] the six fixtures reproduce byte for byte with `sdk = fixture/0.0.0`
- [ ] the conformance suite's in-process target is the Scala SDK; `violation.*` still use the double
- [ ] the harness's names and rules are the Python testkit's
- [ ] the Scala router's `streamlet` descriptor equals the Python one's; its tests are the Python tests
- [ ] `blueprint`, `crd` and the sample skip publishing; `protocol` and `sdk` publish; the POM dependency between them is right
- [ ] `sdk-scala` is gated on a release tag and asks Central before uploading
- [ ] every page with Python code has a Scala tab from tested code; the Scala guide and reference exist; the skills name both SDKs; the install page says what a Scala author needs
- [ ] the README shows both routers from tested code; `just readme-sync` is current
