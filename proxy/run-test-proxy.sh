#!/bin/bash
COMPONENT=Proxy
echo "$(date "+%F %X.%3N") I $(basename "$0"):${LINENO} $$ ${COMPONENT}:StartScript {} Start ${COMPONENT} service"

if [ -z "$SOLANA_URL" ]; then
  echo "$(date "+%F %X.%3N") I $(basename "$0"):${LINENO} $$ ${COMPONENT}:StartScript {} SOLANA_URL is not set"
  exit 1
fi

solana config set -u $SOLANA_URL
ln -s /opt/proxy/operator-keypairs/id?*.json /root/.config/solana/

/spl/bin/create-test-accounts.sh 1

export NUM_ACCOUNTS=30
/spl/bin/create-test-accounts.sh $NUM_ACCOUNTS &

proxy/run-proxy.sh
