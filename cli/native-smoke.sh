#!/usr/bin/env bash
# Smoke-tests a native build of the CLI: `cli/native-smoke.sh <binary> [expected-version] [jvm-flow]`.
#
# A native image builds successfully with a resource or a class missing, and the command that needed
# it then answers with nothing, or with an exception, at run time. So this runs the binary and asks
# it for each thing the image has to carry — the version, the usage, a verification read from disk,
# a resource written as YAML, the settings `flow local` writes, a reset that reaches the Kubernetes
# client — rather than trusting the build. Given a JVM-built `flow` as well, every `verify`,
# `generate` and `local` output is diffed against it byte for byte. The suite run against the binary (`sbt cli/test -Dflow.cli.binary=…`) is the
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
[[ "$version" =~ ^flow\ [^,]+,\ descriptor\ [0-9.]+$ ]] || fail "version answered '$version'"
if [ -n "$expected" ] && [[ "$version" != "flow $expected, descriptor "* ]]; then
  fail "version is '$version', expected 'flow $expected, descriptor …'"
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

# ── verify, generate and local, on every blueprint of the sample ──────────────
# The image names are placeholders the command only writes into the resource; nothing pulls them,
# and `flow local` connects to nothing.
blueprints() { find samples/cart-pipeline -name blueprint.conf -not -path '*/target/*' | sort; }
run_sample() { # <flow> <blueprint.conf> <outdir>
  local flow="$1" blueprint="$2" out="$3" dir; dir="$(dirname "$2")"; mkdir -p "$out"
  local args=(--descriptors "$dir/flow") images=() streamlet
  while read -r streamlet; do images+=(--image "$streamlet=smoke/$streamlet:0"); done < <(
    sed -n 's/^ *\([a-z][a-z0-9-]*\) *= *\([a-z][a-z0-9-]*\) *$/\1/p' "$blueprint" | grep -v '^name$')
  "$flow" verify "$blueprint" "${args[@]}" > "$out/verify.out" 2> "$out/verify.err"
  "$flow" generate "$blueprint" "${args[@]}" "${images[@]}" --version smoke -n smoke \
    > "$out/generate.out" 2> "$out/generate.err"
  "$flow" local "$blueprint" "${args[@]}" --bootstrap-servers localhost:9092 \
    --ankka-bootstrap-servers localhost:9094 -o "$out/local" > "$out/local.out" 2> "$out/local.err"
}
name_of() { echo "$1" | sed 's|^samples/||; s|/blueprint.conf$||; s|/|-|g'; }
while read -r blueprint; do
  name="$(name_of "$blueprint")"; out="$work/native/$name"
  run_sample "$bin" "$blueprint" "$out" || fail "$name: verify, generate or local failed: $(cat "$out"/*.err 2>/dev/null)"
  grep -q '^verified:' "$out/verify.out" || fail "$name: verify did not say verified: $(cat "$out/verify.out")"
  grep -q '^kind: AnkkaFlow' "$out/generate.out" || fail "$name: generate wrote no AnkkaFlow resource: the image cannot write YAML"
  grep -q 'smoke/' "$out/generate.out" || fail "$name: generate wrote the resource without its images"
  ls "$out"/local/*/streamlet.properties > /dev/null 2>&1 || fail "$name: local wrote no streamlet.properties"
  grep -q 'bootstrap.servers' "$out"/local/*/streamlet.properties || fail "$name: local wrote settings without a broker"
  echo "sample     $name: verify, generate and local"
done < <(blueprints)

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

# ── mcp: the server, the docs inside the binary, a tool call ─────────────────
# The server reads its pages and tools from the image; a page missing from the image is a server
# listing fewer resources, and a class fabric8 or the YAML writer reaches through a tool is only
# found by calling one.
mcp_in="$work/mcp.in"; mcp_out="$work/mcp.out"
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"smoke","version":"0"}}}' \
  '{"jsonrpc":"2.0","id":2,"method":"resources/list"}' \
  "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\",\"params\":{\"name\":\"verify_blueprint\",\"arguments\":{\"blueprint\":\"$PWD/samples/cart-pipeline/blueprint.conf\",\"descriptors\":\"$PWD/samples/cart-pipeline/flow\"}}}" \
  > "$mcp_in"
(cd "$work" && "$bin" mcp < "$mcp_in" > "$mcp_out" 2> "$work/mcp.err") || fail "flow mcp exited $?: $(cat "$work/mcp.err")"
grep -q '"protocolVersion":"2025-06-18"' "$mcp_out" || fail "mcp did not initialize: $(head -c 300 "$mcp_out")"
pages="$(grep -o 'ankka-flow://docs/' "$mcp_out" | wc -l | tr -d ' ')"
[ "$pages" -gt 0 ] || fail "mcp lists no documentation pages: the image is missing ankka-flow/docs"
grep -q 'verified: 2 streamlets' "$mcp_out" || fail "mcp's verify_blueprint did not verify: $(tail -c 400 "$mcp_out")"
"$bin" mcp install --scope project --dir "$work/mcp-project" > /dev/null || fail "mcp install --scope project failed"
grep -q '"command": "flow"' "$work/mcp-project/.mcp.json" || fail "mcp install wrote no flow server: $(cat "$work/mcp-project/.mcp.json")"
echo "mcp        initialized, $pages pages, verify_blueprint answered, install wrote .mcp.json"

# ── the JVM build, byte for byte ─────────────────────────────────────────────
if [ -n "$jvm" ]; then
  while read -r blueprint; do
    name="$(name_of "$blueprint")"
    run_sample "$jvm" "$blueprint" "$work/jvm/$name" || fail "$name: the JVM build failed where the binary did not"
    for f in verify.out verify.err generate.out generate.err; do
      cmp -s "$work/native/$name/$f" "$work/jvm/$name/$f" \
        || fail "$name: $f differs between the binary and the JVM build:
$(diff "$work/jvm/$name/$f" "$work/native/$name/$f" | head -10)"
    done
    diff -r "$work/jvm/$name/local" "$work/native/$name/local" > "$work/local.diff" \
      || fail "$name: local's settings differ between the binary and the JVM build: $(head -10 "$work/local.diff")"
  done < <(blueprints)
  echo "jvm        every verify, generate and local output identical to the JVM build's"
fi
echo "native smoke: ok"
