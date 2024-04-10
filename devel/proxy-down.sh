#!/bin/bash

set -xeou pipefail

export DOCKERHUB_ORG_NAME=${DOCKERHUB_ORG_NAME:-neonlabsorg}
export FAUCET_COMMIT=${FAUCET_COMMIT:-latest}
export NEON_EVM_COMMIT=${NEON_EVM_COMMIT:-latest}
export PROXY_REVISION=${PROXY_REVISION:-local}
export PROJECT_NAME=${PROJECT_NAME:-local}
export REVISION=${REVISION:-$PROXY_REVISION}

BINDIR="$(dirname $BASH_SOURCE)"

if [ -f /.dockerenv ]; then
    if [ ! -f /usr/bin/pkill ]; then
        # one-time install, so we can kill without dangling children
        apt-get update
        apt-get install -y psmisc
    fi

    pkill -f 'python3\ \-m\ proxy|bash.*run(-test)?-proxy.sh'
else
    CONTAINERS_RUNNING=$(docker-compose ls -q --filter name=${PROJECT_NAME})

    if [ "$CONTAINERS_RUNNING" ]; then
        docker exec -it "${PROJECT_NAME}-proxy-1" /opt/devel/proxy-down.sh
    fi
fi