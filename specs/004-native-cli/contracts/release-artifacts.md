# Contract: what a release publishes for the CLI

## On the tag's GitHub release

Four assets and four checksum files, named exactly:

```text
ankka-flow-cli-<version>-macos-arm64.tar.gz      ankka-flow-cli-<version>-macos-arm64.tar.gz.sha256
ankka-flow-cli-<version>-macos-x64.tar.gz        ankka-flow-cli-<version>-macos-x64.tar.gz.sha256
ankka-flow-cli-<version>-linux-arm64.tar.gz      ankka-flow-cli-<version>-linux-arm64.tar.gz.sha256
ankka-flow-cli-<version>-linux-x64.tar.gz        ankka-flow-cli-<version>-linux-x64.tar.gz.sha256
```

Each archive holds one file, `flow`, with the executable bit set. Each `.sha256` holds the output
of `shasum -a 256 <archive>`, so `shasum -a 256 -c <archive>.sha256` verifies it.

The release page is created by the workflow for the tag if it does not exist (`--verify-tag`, with
generated notes); a leg run again uploads with `--clobber`.

## Through the tap

```bash
brew install thinkmorestupidless/tap/ankka-flow
flow version
```

```text
flow <version>, protocol <protocol>
```

The formula is `Formula/ankka-flow.rb` in `thinkmorestupidless/homebrew-tap`, beside `ankka.rb`.
Its canonical source is `homebrew/Formula/ankka-flow.rb` here, with `0.0.0` in each `url` (no
`version` line: Homebrew reads the version from the url, and `brew audit --strict` refuses a line
that repeats it) and a zeroed `sha256` per platform, each marked by a trailing `# <platform>`
comment the job replaces.
The job commits the written formula to a clone of the tap and pushes without `--force`, retrying
once after a fetch on a rejected push. It never writes any other file of the tap.

## The jobs and what each waits on

| Job | Needs | Does |
|---|---|---|
| `release-page` | — | creates the tag's release if absent |
| `cli-native` (×4) | `release-page` | refuses a dirty tree; `sbt cli/stage`; builds the image; runs the suite against it; runs the smoke script with the diff against the JVM build; uploads the archive and checksum |
| `homebrew` | all four `cli-native` | writes and pushes the formula |
| `images`, `sdk-python`, `marketplace` | as today | unchanged, and not waiting on the above |

A failed leg fails `homebrew` and nothing else. A leg's failure message names its platform.

## Secrets outside the repository

| Secret | Used by | Scope |
|---|---|---|
| `HOMEBREW_TAP_TOKEN` | `homebrew` | contents: write on `thinkmorestupidless/homebrew-tap` |

The workflow's own token uploads the assets and creates the release page.

## In ankka, before the first release here

ankka's `homebrew` job force-pushes a subtree of its `homebrew/` directory to the tap's `main`,
which would remove `ankka-flow.rb`. It is changed to the same clone-write-commit-push as this one
before ankka-flow's first release with a formula.
