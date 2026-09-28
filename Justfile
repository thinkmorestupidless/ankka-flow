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

# Check every relative link in docs/ and README.md.
docs:
    python3 scripts/check-links.py
