#!/usr/bin/env bash
# Builds the images, loads them into the kind cluster, and installs the CRD, the operator and a
# development Kafka. Refuses to touch any context but the kind cluster it is named for.
set -euo pipefail

cluster="${ANKKA_FLOW_KIND_CLUSTER:-ankka}"
context="kind-${cluster}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "${here}/.." && pwd)"

current="$(kubectl config current-context)"
if [[ "${current}" != "${context}" ]]; then
  echo "refusing: the current context is '${current}', not '${context}'" >&2
  exit 1
fi

cd "${root}"
sbt -batch "sidecar/docker:publishLocal" "operator/docker:publishLocal" sampleImage
for image in ankka-flow-operator:latest ankka-flow-sidecar:latest sample-cart-router:latest; do
  kind load docker-image --name "${cluster}" "${image}"
done

kubectl apply --server-side -f kustomization/components/crd/ankkaflow.yaml
kubectl wait --for=condition=Established crd/ankkaflows.flow.ankka.thinkmorestupidless.com --timeout=60s
kubectl kustomize kustomization/overlays/local | kubectl apply --server-side --force-conflicts -f -
kubectl -n ankka-flow rollout restart deployment/ankka-flow-operator
kubectl -n ankka-flow rollout status deployment/ankka-flow-operator --timeout=180s
kubectl -n kafka rollout status statefulset/kafka --timeout=300s
echo "installed: the operator in 'ankka-flow', Kafka at kafka.kafka.svc:9092 (the 'default' cluster)"
