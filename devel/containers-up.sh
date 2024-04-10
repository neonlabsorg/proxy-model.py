#!/bin/bash

set -xeou pipefail

export CONTRACTS_BRANCH=${CONTRACTS_BRANCH:-develop}
export DOCKERHUB_ORG_NAME=${DOCKERHUB_ORG_NAME:-neonlabsorg}
export FAUCET_COMMIT=${FAUCET_COMMIT:-latest}
export NEON_EVM_COMMIT=${NEON_EVM_COMMIT:-latest}
export PROXY_REVISION=${PROXY_REVISION:-local}
export PROJECT_NAME=${PROJECT_NAME:-local}
export REVISION=${REVISION:-$PROXY_REVISION}
export VIRTUAL_ENV=${VIRTUAL_ENV:-}

BINDIR="$(dirname $BASH_SOURCE)"
COMPOSE_OVERRIDES=""
CONTRACTS_URL="https://github.com/neonlabsorg/neon-evm/archive/refs/heads/$CONTRACTS_BRANCH.tar.gz"

if  [ -z "$VIRTUAL_ENV" ]; then
    python3 -m venv "$BINDIR/../.venv"
    source "$BINDIR/../.venv/bin/activate"
    pip install -r requirements.txt
fi

if [ "$NEON_EVM_COMMIT" != "local" ]; then
    docker pull "neonlabsorg/evm_loader:$NEON_EVM_COMMIT"
fi

if [ "$PROXY_REVISION" == "local" ]; then
    COMPOSE_OVERRIDES="-f $BINDIR/../docker-compose/docker-compose-local-development.yml"

    if [ ! -d "$BINDIR/../contracts" ]; then
        mkdir -p "$BINDIR/../contracts"
        curl -L $CONTRACTS_URL | tar -xz -C "$BINDIR/../contracts"
        mv $BINDIR/../contracts/neon-evm-develop/solidity/* "$BINDIR/../contracts"
        rm -rf "$BINDIR/../contracts/neon-evm-develop"
    fi

    docker build \
        -t "$DOCKERHUB_ORG_NAME/proxy:$PROXY_REVISION" \
        -f "$BINDIR/../Dockerfile" \
        --build-arg "NEON_EVM_COMMIT=$NEON_EVM_COMMIT" \
        --build-arg "DOCKERHUB_ORG_NAME=$DOCKERHUB_ORG_NAME" \
        --build-arg "PROXY_REVISION=$PROXY_REVISION" \
        "$BINDIR/.."
fi

 docker-compose \
    -f "$BINDIR/../docker-compose/docker-compose-ci.yml" \
    $COMPOSE_OVERRIDES \
    -p "$PROJECT_NAME" \
    up