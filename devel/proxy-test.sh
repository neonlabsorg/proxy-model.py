#!/bin/bash

# Note: This script runs the tests within an running proxy
#       container that has a running proxy process, but it does
#       not start the proxy container itself and does not start the
#       proxy process either.
#
#       To start the proxy container and process, use `./devel/proxy-up.sh`.
#

set -euo pipefail

export DOCKERHUB_ORG_NAME=${DOCKERHUB_ORG_NAME:-neonlabsorg}
export FAUCET_COMMIT=${FAUCET_COMMIT:-latest}
export NEON_EVM_COMMIT=${NEON_EVM_COMMIT:-latest}
export PROXY_REVISION=${PROXY_REVISION:-local}
export PROJECT_NAME=${PROJECT_NAME:-local}
export REVISION=${REVISION:-$PROXY_REVISION}

BINDIR="$(dirname $BASH_SOURCE)"

if [ -f /.dockerenv ]; then
    /opt/proxy/deploy-test.sh
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
        docker exec -it "${PROJECT_NAME}-proxy-1" /opt/proxy/deploy-test.sh
    fi
fi