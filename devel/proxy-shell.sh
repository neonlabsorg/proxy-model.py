#!/bin/bash

set -xeou pipefail

export DOCKERHUB_ORG_NAME=${DOCKERHUB_ORG_NAME:-neonlabsorg}
export FAUCET_COMMIT=${FAUCET_COMMIT:-latest}
export NEON_EVM_COMMIT=${NEON_EVM_COMMIT:-latest}
export PROXY_REVISION=${PROXY_REVISION:-local}
export PROJECT_NAME=${PROJECT_NAME:-local}
export REVISION=${REVISION:-$PROXY_REVISION}

if [ -f /.dockerenv ]; then
    set +x

    echo "error: cannot run this script inside a container"
    exit 1
else
    PROXY_CONTAINER=$(docker ps -q -f name=${PROJECT_NAME}-proxy-1)

    if [ -z "$PROXY_CONTAINER" ]; then
        set +x

        echo "error: proxy container not running"
        echo
        echo "To start the proxy container:"
        echo
        echo "  ./devel/proxy-up.sh"
        echo
        exit 1
    else
        docker exec -it "${PROJECT_NAME}-proxy-1" /bin/bash
    fi
fi