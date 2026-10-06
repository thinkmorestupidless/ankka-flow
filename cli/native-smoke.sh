#!/usr/bin/env bash
# Smoke-tests a native build of the CLI: `cli/native-smoke.sh <binary> [expected-version] [jvm-flow]`.
#
# A native image builds successfully with a resource or a class missing, and the command that needed
# it then answers with nothing, or with an exception, at run time. So this runs the binary and asks
# it for each thing the image has to carry — the version, the usage, a verification read from disk,
# a resource written as YAML, a reset that reaches the Kubernetes client — rather than trusting the
# build. Given a JVM-built `flow` as well, every `verify` and `generate` output is diffed against it
# byte for byte. The suite run against the binary (`sbt cli/test -Dflow.cli.binary=…`) is the
# other, fuller check; this one is fast and names what is missing.
set -euo pipefail

bin="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
expected="${2:-}"
jvm="${3:-}"
[ -n "$jvm" ] && jvm="$(cd "$(dirname "$jvm")" && pwd)/$(basename "$jvm")"
cd "$(dirname "${BASH_SOURCE[0]}")/.."
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
export KUBECONFIG="$work/none.kubeconfig"   # never the developer's own cluster

fail() { echo "native smoke: $*" >&2; exit 1; }

# ── version ──────────────────────────────────────────────────────────────────
version="$("$bin" version)"
[[ "$version" =~ ^flow\ [^,]+,\ protocol\ [0-9.]+$ ]] || fail "version answered '$version'"
if [ -n "$expected" ] && [[ "$version" != "flow $expected, protocol "* ]]; then
  fail "version is '$version', expected 'flow $expected, protocol …'"
fi
echo "version    $version"

# ── usage and exit codes ─────────────────────────────────────────────────────
"$bin" --help > "$work/help" 2>&1 || fail "--help exited $?"
grep -q 'verify' "$work/help" || fail "--help does not list the commands: $(head -3 "$work/help")"
code=0; "$bin" > "$work/noargs" 2>&1 || code=$?
[ "$code" = 2 ] || fail "no arguments exited $code rather than 2"
grep -q 'verify' "$work/noargs" || fail "no arguments printed no usage"
code=0; "$bin" --no-such-flag > /dev/null 2>&1 || code=$?
[ "$code" = 2 ] || fail "a wrong flag exited $code rather than 2"
echo "usage      help, no arguments, a wrong flag"

# ── verify and generate, on every sample ─────────────────────────────────────
# The image name is a placeholder the command only writes into the resource; nothing pulls it.
run_sample() { # <flow> <sample> <outdir>
  local flow="$1" sample="$2" out="$3"; mkdir -p "$out"
  local args=(--descriptors "$sample/flow")
  [ -f "$sample/k8s/in-cluster.conf" ] && args+=(--conf "$sample/k8s/in-cluster.conf")
  local images=() streamlet
  while read -r streamlet; do images+=(--image "$streamlet=smoke/$streamlet:0"); done < <(
    sed -n 's/^ *\([a-z][a-z0-9-]*\) *= *\([a-z][a-z0-9-]*\) *$/\1/p' "$sample/blueprint.conf" | grep -v '^name$')
  "$flow" verify "$sample/blueprint.conf" "${args[@]}" > "$out/verify.out" 2> "$out/verify.err"
  "$flow" generate "$sample/blueprint.conf" "${args[@]}" "${images[@]}" --version smoke -n smoke \
    > "$out/generate.out" 2> "$out/generate.err"
}
for sample in samples/*/; do
  sample="${sample%/}"; name="$(basename "$sample")"
  [ -f "$sample/blueprint.conf" ] || continue
  run_sample "$bin" "$sample" "$work/native/$name" || fail "$name: verify or generate failed: $(cat "$work/native/$name"/*.err 2>/dev/null)"
  grep -q '^verified:' "$work/native/$name/verify.out" || fail "$name: verify did not say verified: $(cat "$work/native/$name/verify.out")"
  grep -q '^kind: AnkkaFlow' "$work/native/$name/generate.out" || fail "$name: generate wrote no AnkkaFlow resource: the image cannot write YAML"
  grep -q 'smoke/' "$work/native/$name/generate.out" || fail "$name: generate wrote the resource without its images"
  echo "sample     $name: verify and generate"
done

# ── reset: the Kubernetes client must load and try ───────────────────────────
cat > "$KUBECONFIG" <<'K'
apiVersion: v1
kind: Config
clusters: [{name: nowhere, cluster: {server: http://127.0.0.1:1}}]
contexts: [{name: nowhere, context: {cluster: nowhere, user: nobody}}]
current-context: nowhere
users: [{name: nobody, user: {}}]
K
code=0; "$bin" reset smoke -n smoke > "$work/reset.out" 2> "$work/reset.err" || code=$?
[ "$code" = 1 ] || fail "reset against nowhere exited $code: $(cat "$work/reset.err")"
# `flow` answers with the client's own words for a request that failed, which is the proof that the
# client loaded the kubeconfig, built itself and tried; a binary missing something answers with an
# exception, or with nothing.
grep -q 'reset failed: Operation: \[get\] *for kind: \[AnkkaFlow\]\|cannot reach Kubernetes' "$work/reset.err" \
  || fail "reset did not reach the Kubernetes client: $(cat "$work/reset.err")"
if grep -q 'Exception' "$work/reset.err"; then fail "reset answered with an exception: $(head -3 "$work/reset.err")"; fi
echo "reset      the client loaded the kubeconfig and tried 127.0.0.1:1"

# ── the JVM build, byte for byte ─────────────────────────────────────────────
if [ -n "$jvm" ]; then
  for sample in samples/*/; do
    sample="${sample%/}"; name="$(basename "$sample")"
    [ -f "$sample/blueprint.conf" ] || continue
    run_sample "$jvm" "$sample" "$work/jvm/$name" || fail "$name: the JVM build failed where the binary did not"
    for f in verify.out verify.err generate.out generate.err; do
      cmp -s "$work/native/$name/$f" "$work/jvm/$name/$f" \
        || fail "$name: $f differs between the binary and the JVM build:
$(diff "$work/jvm/$name/$f" "$work/native/$name/$f" | head -10)"
    done
  done
  echo "jvm        every verify and generate output identical to the JVM build's"
fi
echo "native smoke: ok"
