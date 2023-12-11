RELEASE := 'v23.09'
RELEASESEM := 'v1.6.2'

all: format lint build

RUNTRANSFORM=sh -c '\
  docker run \
	-e RAW_KEYSPACE=$$1_raw_dev \
	-e TGT_KEYSPACE=$$1_transformed_dev \
	-e NETWORK=$$1 \
	-p 4040:4040 \
	--network="host" \
	graphsense-spark ./submit.sh' RUNTRANSFORM

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
	sbt scalafix

build:
	sbt package

build-fat:
	sbt assembly

build-docker:
	docker build . -t graphsense-spark

run-docker-eth-transform-local: build-docker
	${RUNTRANSFORM} eth

run-docker-ltc-transform-local: build-docker
	${RUNTRANSFORM} ltc

run-docker-btc-transform-local: build-docker
	${RUNTRANSFORM} btc

run-docker-zec-transform-local: build-docker
	${RUNTRANSFORM} zec

run-docker-bch-transform-local: build-docker
	${RUNTRANSFORM} bch

run-docker-trx-transform-local: build-docker
	${RUNTRANSFORM} trx

tag-version:
	-git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASE) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)
	git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASESEM) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)


.PHONY: all test lint format build tag-version start-local-cassandra stop-local-cassandra run-local-transform build-docker test-account test-utxo