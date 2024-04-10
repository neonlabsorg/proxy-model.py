#!/bin/bash

set -xeou pipefail

export DOCKERHUB_ORG_NAME=${DOCKERHUB_ORG_NAME:-neonlabsorg}
export FAUCET_COMMIT=${FAUCET_COMMIT:-latest}
export NEON_EVM_COMMIT=${NEON_EVM_COMMIT:-latest}
export PROXY_REVISION=${PROXY_REVISION:-local}
export PROJECT_NAME=${PROJECT_NAME:-local}
export REVISION=${REVISION:-$PROXY_REVISION}

if [ -f /.dockerenv ]; then
    # temporary hack to fix stale plugin cache on restart
    rm -Rf /opt/proxy/plugin/__pycache__/*

    /opt/proxy/run-test-proxy.sh
else
    CONTAINERS_RUNNING=$(docker-compose ls -q --filter name=${PROJECT_NAME})

    if [ -z "$CONTAINERS_RUNNING" ]; then
        set +x

        echo "error: containers not running"
        echo
        echo "To start the containers:"
        echo
        echo "  ./devel/containers-up.sh"
        echo
        exit 1
    else
        docker exec -it "${PROJECT_NAME}-proxy-1" /opt/devel/proxy-up.sh
    fi
fi