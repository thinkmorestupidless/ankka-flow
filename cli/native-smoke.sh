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

# ── init: the templates are inside the binary ────────────────────────────────
# `flow init` reads its templates as resources; a template file the image left out is a project
# missing a file, so each project is checked for the files every one must have, its blueprint is
# verified against its descriptor by the binary itself, and — given the JVM build — the two trees
# must be identical.
for language in scala python; do
  out="$work/init/native/$language/greeter"
  TIMEFORMAT=%R
  seconds="$( { time "$bin" init greeter --language "$language" --dir "$out" > "$work/init-$language.out" 2>&1; } 2>&1 )" \
    || fail "init $language failed: $(cat "$work/init-$language.out")"
  for f in blueprint.conf flow/descriptor.json flow/streamlet.conf docker-compose.yml k8s/in-cluster.conf \
           README.md .gitignore .github/workflows/ci.yml .claude/skills/ankka-flow/SKILL.md; do
    [ -f "$out/$f" ] || fail "init $language wrote no $f: the image is missing a template file"
  done
  case "$language" in
    scala)  [ -f "$out/build.sbt" ] && [ -f "$out/src/main/scala/greeter/Greeter.scala" ] || fail "init scala wrote no build or streamlet" ;;
    python) [ -f "$out/pyproject.toml" ] && [ -f "$out/src/greeter/streamlet.py" ] || fail "init python wrote no build or streamlet" ;;
  esac
  if grep -rIl --exclude-dir=.claude '{{[a-z_]*}}' "$out" | grep -v '\.github/' | grep -q .; then
    fail "init $language left a token: $(grep -rIl --exclude-dir=.claude '{{[a-z_]*}}' "$out" | head -3)"
  fi
  "$bin" verify "$out/blueprint.conf" --descriptors "$out/flow" > /dev/null 2> "$work/init-verify.err" \
    || fail "init $language: its blueprint does not verify: $(cat "$work/init-verify.err")"
  awk -v s="$seconds" 'BEGIN { exit !(s < 1.0) }' || fail "init $language took ${seconds}s, more than a second"
  echo "init       $language: $(find "$out" -type f | wc -l | tr -d ' ') files in ${seconds}s, its blueprint verified"
done

# ── mcp: the server, the docs inside the binary, a tool call ─────────────────
# The server reads its pages and tools from the image; a page missing from the image is a server
# listing fewer resources, and a class fabric8 or the YAML writer reaches through a tool is only
# found by calling one.
mcp_in="$work/mcp.in"; mcp_out="$work/mcp.out"
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"smoke","version":"0"}}}' \
  '{"jsonrpc":"2.0","id":2,"method":"resources/list"}' \
  "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\",\"params\":{\"name\":\"verify_blueprint\",\"arguments\":{\"blueprint\":\"$PWD/samples/cart-router/blueprint.conf\",\"descriptors\":\"$PWD/samples/cart-router/flow\"}}}" \
  > "$mcp_in"
(cd "$work" && "$bin" mcp < "$mcp_in" > "$mcp_out" 2> "$work/mcp.err") || fail "flow mcp exited $?: $(cat "$work/mcp.err")"
grep -q '"protocolVersion":"2025-06-18"' "$mcp_out" || fail "mcp did not initialize: $(head -c 300 "$mcp_out")"
pages="$(grep -o 'ankka-flow://docs/' "$mcp_out" | wc -l | tr -d ' ')"
[ "$pages" -gt 0 ] || fail "mcp lists no documentation pages: the image is missing ankka-flow/docs"
grep -q 'verified: 1 streamlets' "$mcp_out" || fail "mcp's verify_blueprint did not verify: $(tail -c 400 "$mcp_out")"
"$bin" mcp install --scope project --dir "$work/mcp-project" > /dev/null || fail "mcp install --scope project failed"
grep -q '"command": "flow"' "$work/mcp-project/.mcp.json" || fail "mcp install wrote no flow server: $(cat "$work/mcp-project/.mcp.json")"
echo "mcp        initialized, $pages pages, verify_blueprint answered, install wrote .mcp.json"

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
  for language in scala python; do
    "$jvm" init greeter --language "$language" --dir "$work/init/jvm/$language/greeter" > /dev/null 2>&1 \
      || fail "the JVM build's init $language failed where the binary's did not"
    diff -r "$work/init/jvm/$language" "$work/init/native/$language" > "$work/init.diff" \
      || fail "init $language: the binary's project differs from the JVM build's: $(head -10 "$work/init.diff")"
  done
  echo "jvm        every verify and generate output, and both init projects, identical to the JVM build's"
fi
echo "native smoke: ok"
