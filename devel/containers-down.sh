#!/bin/bash

set -xeou pipefail

export DOCKERHUB_ORG_NAME=${DOCKERHUB_ORG_NAME:-neonlabsorg}
export FAUCET_COMMIT=${FAUCET_COMMIT:-latest}
export NEON_EVM_COMMIT=${NEON_EVM_COMMIT:-latest}
export PROXY_REVISION=${PROXY_REVISION:-local}
export PROJECT_NAME=${PROJECT_NAME:-local}
export REVISION=${REVISION:-$PROXY_REVISION}
export VIRTUAL_ENV=${VIRTUAL_ENV:-}

BINDIR="$(dirname $BASH_SOURCE)"

docker-compose \
    -f "$BINDIR/../docker-compose/docker-compose-ci.yml" \
    -p "$PROJECT_NAME" \
    down