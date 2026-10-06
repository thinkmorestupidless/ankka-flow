# Contract: the build, CI and the release

## Modules

| Module | Directory | Scala | Depends on | Published as |
|---|---|---|---|---|
| `protocol` | `protocol/` | 3.3 LTS | — | `ankka-flow-protocol_3` |
| `sdk` | `sdks/scala/` | 3.3 LTS | `protocol` | `ankka-flow-sdk_3` |
| `cartRouterScala` | `samples/cart-router-scala/` | 3.3 LTS | `sdk` | not published; image `sample-cart-router-scala` |
| `sidecar` | `sidecar/` | 3.9 | `protocol`, `sdk % "test->compile"` | image only |
| `blueprint`, `crd`, `operator`, `cli`, root | as today | 3.9 | as today | `publish / skip` |

`V.scalaLts` in `project/Dependencies.scala` names the LTS release; the three LTS modules set
`scalaVersion := V.scalaLts` and drop `-source:3.7`. `sdk` has `BuildInfoPlugin` with `version`
and `protocolVersion`. The sample has `JavaAppPackaging`, `DockerPlugin`, the repository's
`dockerSettings`, `Compile / mainClass := Some("cart.Main")`, and the input tasks `descriptor` and
`descriptorCheck`. The rules table in `CLAUDE.md` gains `sdk → protocol`, `sidecar → sdk (test)`,
`cartRouterScala → sdk`.

## Commands

```bash
sbt sdk/test                                   # fixtures, harness, server, Descriptor main
sbt 'sidecar/testOnly *ConformanceSuite'       # in-process: the Scala SDK's reference streamlet
sbt sdkConformance                             # alias of the line above (FR-009's one command)
sbt 'sdk/Test/runMain com.thinkmorestupidless.ankka.flow.sdk.conformance.ConformanceMain 9010'   # serve the reference for -Dflow.conformance.target
sbt cartRouterScala/test cartRouterScala/descriptorCheck
sbt cartRouterScala/descriptor                 # rewrite samples/cart-router-scala/flow/descriptor.json
sbt cartRouterScala/run                        # the router on 127.0.0.1:9010, for the laptop loop
sbt cartRouterScala/docker:publishLocal        # sample-cart-router-scala:<version> and :latest
sbt docker:publishLocal sampleImage            # as today, now four images
sbt -Dflow.release.local=/tmp/m2 publishSigned # the publish rehearsal into a directory
```

`just` gains `sdk-scala` (`sbt sdk/test sdkConformance cartRouterScala/test cartRouterScala/descriptorCheck`).

## CI (`ci.yml`)

- `build`'s path filter gains `sdks/scala/**` and `samples/cart-router-scala/**`; the job's
  `sbt test` already covers the SDK, the sample and the conformance suite. After it:
  `sbt cartRouterScala/descriptorCheck cartRouterScala/docker:publishLocal`.
- `sdk-python` is unchanged: its diffs of the Python copy stay.
- `docs` (in `docs.yml`) covers the new pages and the README include.

## Release (`release.yml`)

| Job | Needs | Gate | Does |
|---|---|---|---|
| `images` | — | release tag | as today, plus `cartRouterScala/docker:publish` |
| `sdk-scala` | `images` | release tag | clean-tree check; "already on Central?" by `ankka-flow-sdk_3/<version>/*.pom`; `sbt ci-release` with `CI_SONATYPE_RELEASE: sonaBundle`; upload the bundle to the Central Portal as `ankka-flow-<version>`, `publishingType=AUTOMATIC`; fails naming the first secret unset |
| `sdk-python`, `marketplace`, `release-page`, `cli-native`, `homebrew` | as today | as today | unchanged |

A pre-release tag runs none of `images`, `sdk-scala`, `sdk-python`, `marketplace`.

### Secrets outside the repository

| Secret | Used by | What |
|---|---|---|
| `PGP_SECRET` | `sdk-scala` | the signing key, base64 of the armored export |
| `PGP_PASSPHRASE` | `sdk-scala` | its passphrase |
| `SONATYPE_USERNAME` | `sdk-scala` | the Central Portal user token's username |
| `SONATYPE_PASSWORD` | `sdk-scala` | the token's password |

The namespace `com.thinkmorestupidless` is already claimed on the portal by ankka's releases.

## Published artifacts

`com.thinkmorestupidless:ankka-flow-sdk_3:<version>` with a POM dependency on
`com.thinkmorestupidless:ankka-flow-protocol_3:<version>`, both with sources and javadoc jars,
signed. A reader's build:

```scala
libraryDependencies += "com.thinkmorestupidless" %% "ankka-flow-sdk" % "<version>"
```
