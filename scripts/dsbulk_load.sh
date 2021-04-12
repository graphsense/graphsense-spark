#!/bin/bash

DSBULK=dsbulk
cqlsh -f scripts/schema_raw.cql

echo "Loading tagpacks"
$DSBULK load -c json -k tagpacks -t tag_by_address -url src/test/resources/cassandra/test_tags.json --schema.allowMissingFields true
echo "Loading exchange_rates"
$DSBULK load -c json -k eth_raw -t exchange_rates -url src/test/resources/cassandra/test_exchange_rates.json
echo "Loading blocks"
$DSBULK load -c csv -header true -k eth_raw -t block -url src/test/resources/cassandra/test_blocks.csv
echo "Loading transactions"
$DSBULK load -c csv -header true -k eth_raw -t transaction -url src/test/resources/cassandra/transactions.csv
