# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]
### Fixed
- Removed the hard-coded `broadcast(exchangeRates)` join hint from the TRX
  encoded-transactions step. `exchange_rates` has one row per block, so the
  broadcast table grows with chain length; TRX (~75M blocks at 3s block time)
  now builds an 8.6 GiB broadcast relation, exceeding Spark's
  non-configurable 8 GiB broadcast limit and killing the transform 4+ hours
  in with `Cannot broadcast the table that is larger than 8.0 GiB`. The
  explicit hint bypasses `spark.sql.autoBroadcastJoinThreshold`, so no
  configuration could prevent it. Without the hint the planner picks a
  sort-merge join, and AQE (enabled in the production properties) still
  converts back to a broadcast join at runtime whenever the actual rate set
  is small enough. The identical hint in the ETH transformation is kept:
  ETH's ~12s block time puts it around ~23M blocks / ~2.5 GiB, decades from
  the cap. Output is unchanged — this only affects the join strategy.

## [v26.07.0] 2026-07-07
### Added
- **Unpegged token support for the account model (ETH, TRX), mirroring
  graphsense-lib's extended token support.** `token_configuration` entries may
  now have no `peg_currency` (`pegCurrency = None` in the `TokenSet`). Such
  tokens are priced from per-token exchange rates instead of the native-coin
  rate: the transform reads the raw `token_exchange_rates(asset, date)` table
  (populated by the graphsense-lib rates ingest), maps the daily rates to
  per-block rows for every (asset, block) pair that saw a transfer of an
  unpegged token, writes them to the transformed
  `token_exchange_rates(asset, block_id)` table, and uses them to compute the
  fiat values of encoded token transfers (and therefore of all downstream
  aggregates: address/relation token values). Unpegged transfers with no
  fetched rate get zero fiat values, matching the graphsense-lib delta-updater.
  Keyspaces without the new tables (schema migration not applied yet) keep
  working: the read falls back to an empty rate set and the write is skipped,
  each with a warning.

## [v26.06.0] 2026-05-29
### Added
- Optional Cassandra Sidecar bulk-write path, selected with the `--writer`
  argument (`cassandra`, default, or `sidecar`). With `--writer sidecar`,
  transformed tables are written by generating SSTables on the Spark executors
  and streaming them into Cassandra through the Cassandra Sidecar (via the
  cassandra-analytics data source), bypassing the CQL coordinator/commitlog
  write path. The default keeps the Spark Cassandra connector write path
  unchanged.
- Release workflow now builds and attaches both the slim (`sbt package`) and
  fat (`sbt assembly`) jars as GitHub Release assets, so consumers can download
  a versioned jar from a public, token-free URL instead of GitHub Packages.
### Changed
- `build.sbt`: application runtime dependencies (scallop, spark-cassandra-connector,
  joda-time, web3j, graphframes) moved from `Provided` to compile scope so the
  assembly jar bundles them (graphframes in particular is not on Maven Central).
  Spark (`spark-sql`, `spark-graphx`) and the optional Cassandra Sidecar
  `cassandra-analytics-core` remain `Provided`. `sbt package` / `sbt publish`
  output and the existing spark-submit flow are unchanged.
### Fixed
- `docker/submit.sh` pinned the Spark Cassandra connector (`3.4.1`) and
  graphframes (`spark3.4`) packages to Spark 3.4 artifacts while the build
  targets Spark 3.5.

## [25.07.2] 2026-05-12
### Fixed
- Docker build failed on current `eclipse-temurin:11-jdk` (Ubuntu 25.10) because `apt-key` has been removed. Switched the SBT repository key handling to `gpg --dearmor` + `signed-by=` in `/etc/apt/keyrings/sbt.gpg`.
- `make build-docker` failed under Podman, which does not accept `--provenance=false`. The flag is now only passed when the local `docker` binary advertises support for it.
### Changed
- Bump Spark 3.5.3 → 3.5.8 (latest patch in the 3.5.x line) and pull the tarball from `dlcdn.apache.org` instead of `archive.apache.org` to avoid the rate-limited archive host.
- Bump Hadoop 2.7.7 → 2.10.2 (last patch in the 2.x line, wire/API-compatible) and switch its download from `archive.apache.org` to `dlcdn.apache.org` for the same reason.

## [24.07.0] 2025-06-26
### added
- new supported tokens for eth

## [24.11.1] 2024-12-06
### Fixed
- block difficulty and total difficulty can now be null

## [24.11.0] 2024-11-14
### Changed
- Upgrade to Spark 3.5.3
- Upgrade DataStax Spark Cassandra connector to 3.5.1
### Added
- max-block cli parameter for utxo currencies and eth to test with smaller datasets.

## [24.02.0] 2024-03-04
### Fixed
- excessive logging in container
- tron.trace callvalue overflow int -> bigint
### Changed
- Upgrade to Spark 3.4.2
- Upgrade DataStax Spark Cassandra connector to 3.4.1
- Simplified (record local) calculation of secondary ids
- Simplified (record local) calculation of tx ids (account model currencies)

## [24.01.0] 2024-01-08
### Added
- implemented transform for tron currency
- checkpoints and loading on HDFS
### Changed
- Upgrade to Spark 3.2.4
- Change package name graphsense-ethereum-transformation -> graphsense-spark
- integrated UTXO (BTC, ZEC, LTC, BCH transform)
- revised namespace structure (BREAKING: call is different path, new --network parameter needed!)

## [23.09/1.5.1] 2023-10-25
### Fixed
- duplicated txs ids in `block_transactions`

## [23.06/1.5.0] 2023-06-10
### Changed
- Include Ethereum internal transactions in `address`, `address_relations` tables. [#8](https://github.com/graphsense/graphsense-ethereum-transformation/issues/8)

## [23.01/1.4.0] 2023-03-29
### Changed
- Upgrade to Spark 3.2.3
- Changed handling of missing exchange rates values; don't fill with zeros,
  remove blocks, txs etc instead.

## [23.01/1.3.0] 2023-01-30
### Added
- Token Support for Ethereum stable coin tokens (WETH, USDT, USDC)
- Added Parsing of ETH-logs to support tokens
- Compute contracts from traces table
- Changed schema of `address_transactions`, `address`, `address_relations` to support tokens and their aggregated values.
- Balance table now contains on balance per currency (ETH and tokens)
- New table token configurations containing the supported tokens and their details
- Added scalafmt and scalastyle sbt plugins

## [22.11] 2022-11-24
### Added
- Added columns to `summary_statistics` table

## [22.10] 2022-10-10
### Changed
- Upgraded to Spark 3.2.1
- Updated Spark Cassandra connector to version 3.2.0

## [1.0.0] 2022-07-11
### Changed
- Updated raw Cassandra schema

## [0.5.2] 2022-03-10
### Changed
- Improved balance calculation
### Removed
- Removed tag handling (see graphsense/graphsense-tagpack-tool)

## [0.5.1] 2021-11-29
### Changed
- Upgrade to Spark 3
- Improved Cassandra schema
- Changed package name

### Added
- Added command-line arguments to spark submit script
- Added `trace` table
- Added balance calculation

## [0.5.0] 2021-05-31
### Changed
- Initial release
