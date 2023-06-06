RELEASE := 'v23.06'
RELEASESEM := 'v1.5.0'

all: format lint

stop-local-cassandra:
	docker-compose -f local_transform.compose.yml down

start-local-cassandra: stop-local-cassandra
	docker-compose -f local_transform.compose.yml up cassandra_eth_transform

run-local-transform: stop-local-cassandra
	docker-compose -f local_transform.compose.yml up --build

test:
	sbt test

format:
	sbt scalafmt

lint:
	sbt compile
	sbt scalastyle

build:
	sbt package

tag-version:
	git diff --exit-code && git diff --staged --exit-code && git tag -a $(RELEASE) -m 'Release $(RELEASE)' && git tag -a $(RELEASESEM) -m 'Release $(RELEASE)' || (echo "Repo is dirty please commit first" && exit 1)


.PHONY: all test lint format build tag-version start-local-cassandra stop-local-cassandra run-local-transform