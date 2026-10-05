# Contract: the CLI, its suite and the smoke script

## `flow` is unchanged

Every command, flag, message and exit code of `flow` is as it is today. `Main.run(args, out, err)`
loses its `resetter` parameter; `Reset.kubernetes` builds its client from the environment as a
user's `flow reset` does, which is the only way a binary can be tested through it.

## The build

```bash
sbt cli/stage                              # the JVM build: cli/target/universal/stage/bin/flow
sbt cli/GraalVMNativeImage/packageBin      # the native binary: cli/target/graalvm-native-image/flow
```

The second needs a GraalVM `native-image` on `PATH` or `GRAALVM_HOME` set. What the image must
carry is declared in the jar under `META-INF/native-image/com.thinkmorestupidless/ankka-flow-cli/`:
`native-image.properties` (the flags) and the reachability configuration (reflection, resources,
serialization), so any native build of this jar gets it right.

## The suite against either

```bash
sbt cli/test                                                          # in process
sbt cli/test -Dflow.cli.binary=$PWD/cli/target/graalvm-native-image/flow   # the binary
```

`-Dflow.cli.binary` is listed in `forwardedTestSwitches`. `CliFixtures.flow(args*)` returns
`(code, out, err)` from whichever driver the switch selects. The reset cases set `KUBECONFIG` to a
file naming the mock server for both drivers; no case checks the driver, and no case is skipped by
it. A case that passes in process and fails against the binary is a defect of the image's
configuration.

## The smoke script

```bash
cli/native-smoke.sh <binary> [expected-version] [jvm-flow]
```

| Asks | Fails when |
|---|---|
| `flow version` | the output is not `flow <expected>, protocol <n>` |
| `flow --help`, `flow`, `flow --bad-flag` | the usage text is missing, or the exit codes are not 0, 2, 2 |
| `flow verify` on each sample with its descriptors and `k8s/in-cluster.conf` | the verification does not say `verified:` |
| `flow generate` on each sample with an `--image` | the output is not YAML holding `kind: AnkkaFlow` and the image |
| `flow reset <pipeline>` with `KUBECONFIG` naming an address nothing listens on | the answer is not a connection refusal from the client, which proves the client loaded |
| with a JVM `flow` given: every `verify` and `generate` above, run again with it | any byte differs |

Exit 0 prints one line per check; exit 1 prints the first failure, naming what was missing or
which output differed.

## CI

`cli-native` in `ci.yml`, on `ubuntu-22.04`, when the CLI or what it depends on changes: the
build, the suite against the binary, and the smoke script with the diff. The three other platforms
are built at a release.
