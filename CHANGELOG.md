# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

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
