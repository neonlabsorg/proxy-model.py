ARG NEON_EVM_COMMIT
ARG DOCKERHUB_ORG_NAME

FROM ${DOCKERHUB_ORG_NAME}/evm_loader:${NEON_EVM_COMMIT} AS spl

FROM ubuntu:22.04

WORKDIR /opt

RUN apt update && \
    DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC \
        apt install -y \
            software-properties-common \
            openssl \
            curl \
            parallel \
            netcat-openbsd \
            ca-certificates \
            python3-pip \
            python3-venv \
            postgresql-client && \
    rm -rf /var/lib/apt/lists/*

ENV SSL_URL=http://security.ubuntu.com/ubuntu/pool/main/o/openssl
ENV SSL_VER=1.1.1f-1ubuntu2

RUN \
    curl ${SSL_URL}/libssl1.1_${SSL_VER}_amd64.deb -O && \
    curl ${SSL_URL}/openssl_${SSL_VER}_amd64.deb -O && \
    apt install -y --allow-downgrades \
        ./libssl1.1_${SSL_VER}_amd64.deb \
        ./openssl_${SSL_VER}_amd64.deb && \
    rm -f \
        ./libssl1.1_${SSL_VER}_amd64.deb \
        ./openssl_${SSL_VER}_amd64.deb

COPY ./requirements.txt /opt

RUN python3 -m venv venv && \
    pip3 install --upgrade pip && \
    /bin/bash -c "source venv/bin/activate" && \
    pip3 install -r requirements.txt && \
    pip3 install py-solc-x && \
    python3 -c "import solcx; solcx.install_solc(version='0.7.6')"

COPY --from=spl \
    /root/.local/share/solana/install/active_release/bin/solana \
    /root/.local/share/solana/install/active_release/bin/solana-keygen \
    /cli/bin/

COPY --from=spl \
    /root/.local/share/solana/install/active_release/bin/spl-token \
    /opt/create-test-accounts.sh \
    /opt/neon-cli \
    /opt/evm_loader-keypair.json \
    /spl/bin/
RUN chmod +x /spl/bin/create-test-accounts.sh

# TODO: rename
COPY --from=spl /opt/neon-api /spl/bin/neon-core-api

COPY --from=spl \
    /opt/solidity/ \
    /opt/contracts/

COPY --from=spl \
    /opt/deploy/test_invoke_program/test_invoke_program-keypair.json \
    /spl/bin/

COPY proxy/operator-keypairs/id.json /root/.config/solana/

COPY . /opt
ARG PROXY_REVISION
RUN sed -i 's/NEON_PROXY_REVISION_TO_BE_REPLACED/'"$PROXY_REVISION"'/g' /opt/proxy/neon_rpc_api_model/neon_rpc_api_worker.py

ENV PATH /venv/bin:/cli/bin/:/spl/bin/:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ENV CONFIG="devnet"

EXPOSE 9090/tcp
ENTRYPOINT [ "./proxy/run-proxy.sh" ]
