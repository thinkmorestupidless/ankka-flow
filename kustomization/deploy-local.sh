#!/usr/bin/env bash
# Builds the images, loads them into the kind cluster, and installs the CRD, the operator, a
# development Kafka, MinIO, the Flink Kubernetes Operator and the Secret flink-state that names
# MinIO as the state store. Refuses to touch any context but the kind cluster it is named for.
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
sbt -batch "sidecar/docker:publishLocal" "operator/docker:publishLocal" sampleImage \
  "cartCleaner/docker:publishLocal" "cartCounter/image"
version="$(sbt -batch -Dsbt.supershell=false -error 'print cartCounter/version' | tail -1 | tr '+' '-')"
docker tag "sample-cart-counter:${version}" sample-cart-counter:latest
# A multi-platform image fails `kind load docker-image` on Docker's containerd store; one platform
# saved and loaded as an archive does not.
arch="$(docker version --format '{{.Server.Arch}}')"
for image in ankka-flow-operator:latest ankka-flow-sidecar:latest sample-cart-router:latest \
  sample-cart-cleaner:latest sample-cart-counter:latest; do
  docker save --platform "linux/${arch}" "${image}" | kind load image-archive --name "${cluster}" /dev/stdin
done

# The Flink Kubernetes Operator, from the manifest the cluster suite applies, before the pipeline
# operator so a Flink streamlet is never refused for want of its CRD.
kubectl apply --server-side -f operator/src/test/resources/flink-operator.yaml
kubectl -n flink-operator rollout status deployment/flink-kubernetes-operator --timeout=300s

kubectl apply --server-side -f kustomization/components/crd/ankkaflow.yaml
kubectl wait --for=condition=Established crd/ankkaflows.flow.ankka.thinkmorestupidless.com --timeout=60s
kubectl kustomize kustomization/overlays/local | kubectl apply --server-side --force-conflicts -f -
kubectl -n ankka-flow rollout restart deployment/ankka-flow-operator
kubectl -n ankka-flow rollout status deployment/ankka-flow-operator --timeout=180s
kubectl -n kafka rollout status statefulset/kafka --timeout=300s
kubectl -n minio rollout status deployment/minio --timeout=300s
kubectl -n minio wait --for=condition=complete job/minio-bucket --timeout=300s
kubectl apply --server-side -f samples/cart-pipeline/k8s/flink-state.yaml
echo "installed: the operator in 'ankka-flow', Kafka at kafka.kafka.svc:9092 (the 'default' cluster),"
echo "the Flink operator watching 'shop', and MinIO's bucket flow-state as the state store"
