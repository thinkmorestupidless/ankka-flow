# Thin wrappers. Every recipe is one command or a call to kustomization/deploy-local.sh, which owns
# the logic and the guards; everything here works without `just` installed.

cluster := env_var_or_default("ANKKA_FLOW_KIND_CLUSTER", "ankka")

default:
    @just --list --unsorted

# Format every Scala and sbt file.
fmt:
    sbt scalafmtAll scalafmtSbt

# Refuse unformatted commits in this clone.
hooks:
    git config core.hooksPath .githooks

# Everything except the k3s suite and the sample image it needs.
test:
    sbt -Dflow.cluster.tests=off test

# Everything, kept awake: a sleeping laptop pauses Docker while test deadlines keep counting.
test-all:
    caffeinate -i sbt test

# Proves the commit-after-write guard: the suite must FAIL with the commit moved first (SC-006).
mutation:
    sbt mutationCheck

# Build the `flow` CLI; put it on your PATH with the line this prints.
cli:
    sbt cli/stage
    @echo 'export PATH="{{justfile_directory()}}/cli/target/universal/stage/bin:$PATH"'

# Build the `flow` CLI as one native executable (needs a GraalVM: GRAALVM_HOME, or native-image on PATH),
# run the CLI's suite against it, and smoke-test it against the JVM build: cli/target/graalvm-native-image/flow.
cli-native:
    sbt cli/stage cli/GraalVMNativeImage/packageBin 'cli/test' -Dflow.cli.binary={{justfile_directory()}}/cli/target/graalvm-native-image/flow && cli/native-smoke.sh cli/target/graalvm-native-image/flow "" cli/target/universal/stage/bin/flow

# The sidecar and operator images, and the sample's.
images:
    sbt docker:publishLocal sampleImage

# The Python SDK, end to end.
sdk:
    cd sdks/python && uv sync && uv run python scripts/proto.py && uv run mypy && uv run pytest -q && uv run conformance

# A single-node Kafka on localhost:9094 for repository development.
kafka-up:
    docker compose up -d kafka

kafka-down:
    docker compose down

cluster-create:
    kind create cluster --name {{cluster}} --config kustomization/kind.yaml

cluster-delete:
    kind delete cluster --name {{cluster}}

cluster-status:
    kubectl --context kind-{{cluster}} get pods -n ankka-flow -o wide && kubectl --context kind-{{cluster}} get pods -n kafka

# Build images, load them into kind, install the CRD, operator and a dev Kafka.
deploy:
    ./kustomization/deploy-local.sh

up:
    kind get clusters | grep -qx {{cluster}} || just cluster-create
    kubectl config use-context kind-{{cluster}}
    just deploy

down:
    just cluster-delete

render overlay="local":
    kubectl kustomize kustomization/overlays/{{overlay}}

# A development Neo4j and the Secret `neo4j-local` in `shop`, for a pipeline with a Neo4j merge sink.
neo4j-up:
    kubectl --context kind-{{cluster}} apply -k kustomization/overlays/neo4j && kubectl --context kind-{{cluster}} -n neo4j rollout status statefulset/neo4j --timeout=180s

# Removes Neo4j and its Secret, never the `shop` namespace the overlay also creates.
neo4j-down:
    kubectl --context kind-{{cluster}} delete namespace neo4j --ignore-not-found && kubectl --context kind-{{cluster}} -n shop delete secret neo4j-local --ignore-not-found

# Check every page, then build the site, llms.txt, llms-full.txt, docs-index.json and the skills.
docs:
    uv run --project tools/docs docs build

# Refresh included samples, the generated protocol table and the rendered skills from their sources.
docs-sync:
    uv run --project tools/docs docs sync

# Refresh the README's included samples (the docs tool reads only the pages; this reads the README).
readme-sync:
    uv run --project tools/docs python tools/docs/readme_includes.py

# The site with live reload, while writing.
docs-serve:
    uv run --project tools/docs docs serve

# The living features, the glossary and the specs that name their scenarios, checked against each other.
features:
    .github/features-check.sh
