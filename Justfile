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

# Everything except the k3s suite and the images it needs.
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

# The operator's image and the sample's two.
images:
    sbt operator/docker:publishLocal cartCleaner/docker:publishLocal cartCounter/image

# Both engines' SDKs, end to end: their suites, the Flink testkit, and the commit-after-write mutation.
sdks:
    sbt pekkoSdk/test flinkSdk/test flinkTestkit/test mutationCheck

# A single-node Kafka on localhost:9092, topics created on first use, for development and the laptop walkthrough.
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

# Render the Flink Kubernetes Operator's chart into operator/src/test/resources/flink-operator.yaml.
flink-operator-manifest:
    ./kustomization/flink-operator-manifest.sh

render overlay="local":
    kubectl kustomize kustomization/overlays/{{overlay}}

# Check every page, then build the site, llms.txt, llms-full.txt, docs-index.json and the skills.
docs:
    uv run --project tools/docs docs build

# Refresh included samples and the rendered skills from their sources.
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
