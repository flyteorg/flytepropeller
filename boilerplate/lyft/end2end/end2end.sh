#!/usr/bin/env bash

# WARNING: THIS FILE IS MANAGED IN THE 'BOILERPLATE' REPO AND COPIED TO OTHER REPOSITORIES.
# ONLY EDIT THIS FILE FROM WITHIN THE 'LYFT/BOILERPLATE' REPOSITORY:
#
# TO OPT OUT OF UPDATES, SEE https://github.com/lyft/boilerplate/blob/master/Readme.rst

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

OUT="${DIR}/tmp"
rm -rf ${OUT}
git clone -b ignore-log-errors https://github.com/lyft/flyte.git "${OUT}"

# Create docker secret in flyte namespace
kubectl create secret docker-registry githubpackages -n flyte \
  --docker-server="docker.pkg.github.com" \
  --docker-username="${DOCKER_USERNAME}" \
  --docker-password="${DOCKER_PASSWORD}" \
  --docker-email="localhost@localhost"

kubectl patch serviceaccount default -n flyte -p '{"imagePullSecrets":[{"name":"githubpackages"}]}'

pushd ${OUT}
sed -i.bak -e "s_docker.io/lyft/flytepropeller:v0.2.36_docker.pkg.github.com/${PROPELLER}_g" ${OUT}/kustomize/base/propeller/deployment.yaml
make kustomize
make end2end_execute
popd
