# Quickstart: validating the native `flow` binary

Needs a GraalVM JDK with `native-image` (`sdk install java 25-graalce`, or `GRAALVM_HOME`).

## Tier 1 — the JVM suite, unchanged

```bash
sbt cli/test
```

Proves: the driver seam and the reset cases' kubeconfig change nothing in process (SC-006).

## Tier 2 — the binary, and the suite against it

```bash
sbt cli/stage cli/GraalVMNativeImage/packageBin
sbt cli/test -Dflow.cli.binary=$PWD/cli/target/graalvm-native-image/flow
cli/native-smoke.sh cli/target/graalvm-native-image/flow "" cli/target/universal/stage/bin/flow
```

Proves: every case passes against the binary with none skipped (SC-002); verify and generate are
byte-identical for every sample (SC-003); each thing the image must carry is there.

A case that fails against the binary and not in process is a missing reachability entry: run the
suite under the tracing agent (`sbt cli/test -Dflow.cli.agent=on`, which adds
`-agentlib:native-image-agent=config-merge-dir=…` to the test JVM) and prune the result into
`META-INF/native-image/…/`.

## Tier 3 — the install page, read fresh

```bash
just docs
grep -rn 'sbt cli/stage\|just cli' docs README.md samples/*/README.md
```

Proves: the docs build; the only mentions of building from source are on the contributing page
(SC-005).

## Tier 4 — a release, on a branch's tag

Tag a pre-release (`v0.4.0-rc.1`) on the branch with ankka's tap job already changed (research
R6) and `HOMEBREW_TAP_TOKEN` set:

1. `release-page` creates the release; four `cli-native` legs attach four archives and checksums;
   `homebrew` commits `Formula/ankka-flow.rb` to the tap and `Formula/ankka.rb` is untouched.
2. On a Mac with no JVM: `brew install thinkmorestupidless/tap/ankka-flow`, `flow version` prints
   the tag's version (SC-001). `brew install thinkmorestupidless/tap/ankka` beside it; both work.
3. Download `ankka-flow-cli-<version>-macos-arm64.tar.gz` and its checksum; `shasum -a 256 -c`;
   unpack; `./flow version`.
4. Break one platform on a branch (a bad flag in `native-image.properties` under a platform
   condition) and tag `-rc.2`: three archives attached, `homebrew` not run, the failure names the
   platform (SC-004). Delete the rc tags and their releases afterwards.

## Reviewer's checklist

- [ ] `flow`'s commands, flags, messages and exit codes are unchanged; the suite's case count is
      the same in process as before
- [ ] no case in `cli/test` is skipped or conditional on the driver
- [ ] `-Dflow.cli.binary` and `-Dflow.cli.agent` are in `forwardedTestSwitches`
- [ ] the reachability configuration is pruned to what `flow` uses and says where it came from
- [ ] `images`, `sdk-python` and `marketplace` do not depend on the new jobs
- [ ] the `homebrew` job never force-pushes, and ankka's no longer does
- [ ] `homebrew/Formula/ankka-flow.rb` has `version "0.0.0"` and four zeroed checksums with their
      platform comments
- [ ] the install page says `brew install` first, the archive second, and nothing about sbt
- [ ] `just cli` and `sbt cli/stage` work
