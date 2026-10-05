# Data Model: A Native `flow` Binary

Nothing here is stored. These are the artifacts a release produces and the names they are known
by.

## Native binary

| Field | Rule |
|---|---|
| name | `flow` |
| platform | one of `macos-arm64`, `macos-x64`, `linux-arm64`, `linux-x64` |
| version | the tag's, without `v`; printed by `flow version` as `flow <version>, protocol <protocol>` |
| built from | the tag's commit, from a clean tree, on a runner of its platform |

## Archive

| Field | Rule |
|---|---|
| name | `ankka-flow-cli-<version>-<platform>.tar.gz` |
| contents | the one file `flow`, executable |
| checksum file | `<name>.sha256`, one line, `<hex sha256>  <name>`, as `shasum -a 256` writes it |
| where | an asset of the tag's GitHub release; re-uploading replaces (`--clobber`) |

## Formula

| Field | Rule |
|---|---|
| file | `Formula/ankka-flow.rb` in `thinkmorestupidless/homebrew-tap`; canonical in this repository at `homebrew/Formula/ankka-flow.rb` with `version "0.0.0"` and zeroed checksums |
| installs | `flow` |
| version | the release's |
| per platform | the archive's URL on the release and its sha256 |
| updated | by the `homebrew` job, after all four archives are attached; by commit and push, never force |

## Test driver

| Driver | Chosen by | Runs |
|---|---|---|
| in process | default | `Main.run(args, out, err)` |
| binary | `-Dflow.cli.binary=<path>` | the binary as a subprocess with the same arguments, `KUBECONFIG` from the environment |

Both return `(exit code, stdout, stderr)`. The reset cases give both a kubeconfig naming the mock
server.

## Smoke script

| Input | Output |
|---|---|
| a binary, optionally the expected version, optionally the JVM-built `flow` | exit 0 with one line per thing checked; exit 1 naming the first thing missing, or the first output that differs from the JVM build's |
