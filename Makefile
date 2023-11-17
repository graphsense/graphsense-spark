RELEASE := 'v23.09'
RELEASESEM := 'v1.5.1'

all: format lint

stop-local-cassandra:
	docker-compose -f local_transform.compose.yml down

start-local-cassandra: stop-local-cassandra
	docker-compose -f local_transform.compose.yml up cassandra_eth_transform

run-local-transform: stop-local-cassandra
	docker-compose -f local_transform.compose.yml up --build

test:
	sbt test

test-account:
	sbt test:compile "testOnly org.graphsense.account.*"

test-utxo:
	sbt test:compile "testOnly org.graphsense.utxo.*"

format:
	sbt scalafmt

lint:
	sbt compile
	sbt scalastyle

build:
	sbt package

build-fat:
	sbt assembly

build-docker:
	docker build . -t graphsense-spark

run-docker-eth-tranform-local:
	docker run \
	-e RAW_KEYSPACE=eth_raw_dev \
	-e TGT_KEYSPACE=eth_transformed_dev \
	-e NETWORK=eth \
	graphsense-spark ./submit.sh

tag-version:
	-git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASE) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)
	git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASESEM) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)


.PHONY: all test lint format build tag-version start-local-cassandra stop-local-cassandra run-local-transform build-docker test-account test-utxo