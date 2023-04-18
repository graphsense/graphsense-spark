#!/bin/bash

DSBLKINPATH=$(which dsbulk)
DSBULK=${DSBULK_PATH:-$DSBLKINPATH}

HOST=${CASSANDRA_HOST:-localhost}

echo $CASSANDRA_HOST
echo $DSBULK_PATH

echo "Using dsbulk at $DSBULK import to $HOST"

cqlsh -f scripts/schema_raw.cql

echo "Loading exchange_rates"
$DSBULK load -h $HOST -logDir /tmp -c json -k eth_raw -t exchange_rates -url src/test/resources/cassandra/test_exchange_rates.json
echo "Loading blocks"
$DSBULK load -h $HOST -logDir /tmp -c csv -header true -k eth_raw -t block -url src/test/resources/cassandra/test_blocks.csv
echo "Loading transactions"
$DSBULK load -h $HOST -logDir /tmp -c csv -header true -k eth_raw -t transaction -url src/test/resources/cassandra/transactions.csv --schema.allowMissingFields true
echo "Loading traces"
$DSBULK load -h $HOST -logDir /tmp -c csv -header true -k eth_raw -t trace -url src/test/resources/cassandra/traces.csv
