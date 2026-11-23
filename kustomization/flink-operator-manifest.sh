#!/usr/bin/env bash
# Renders the Flink Kubernetes Operator's Helm chart, at the version project/Dependencies.scala
# names, into operator/src/test/resources/flink-operator.yaml: the manifest the cluster suite and
# deploy-local.sh apply. The webhook is off, so no cert-manager is needed; the operator watches the
# namespace `shop` the suite and the local overlay run pipelines in, and makes the job service
# account `flink` there. Run it on a bump of V.flinkOperator and commit the result.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "${here}/.." && pwd)"
version="$(sed -n 's/.*val flinkOperator *= *"\([^"]*\)".*/\1/p' "${root}/project/Dependencies.scala")"
[[ -n "${version}" ]] || { echo "project/Dependencies.scala names no V.flinkOperator" >&2; exit 1; }

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT
curl -sfL -o "${work}/chart.tgz" \
  "https://archive.apache.org/dist/flink/flink-kubernetes-operator-${version}/flink-kubernetes-operator-${version}-helm.tgz"
tar -xzf "${work}/chart.tgz" -C "${work}"

out="${root}/operator/src/test/resources/flink-operator.yaml"
{
  echo "# The Flink Kubernetes Operator ${version}, rendered from its Helm chart by"
  echo "# kustomization/flink-operator-manifest.sh (just flink-operator-manifest). Do not edit."
  echo "# helm template --include-crds --namespace flink-operator"
  echo "#   --set webhook.create=false --set watchNamespaces={shop}"
  echo "apiVersion: v1"
  echo "kind: Namespace"
  echo "metadata:"
  echo "  name: flink-operator"
  echo "---"
  echo "apiVersion: v1"
  echo "kind: Namespace"
  echo "metadata:"
  echo "  name: shop"
  echo "---"
  helm template flink-kubernetes-operator "${work}/flink-kubernetes-operator" \
    --namespace flink-operator --include-crds \
    --set webhook.create=false --set 'watchNamespaces={shop}'
} > "${out}"
echo "wrote ${out#"${root}/"} (Flink Kubernetes Operator ${version})"
