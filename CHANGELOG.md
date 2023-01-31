# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [23.01/1.3.0] - 2023-01-30
### Added
- Token Support for Ethereum stable coin tokens (WETH, USDT, USDC)
- Added Parsing of eth-logs to support tokens
- compute contracts from traces table
- changed schema of address_transactions, address, address_relations to support tokens and their aggregated values.
- balance table now contains on balance per currency (ETH and tokens)
- new table token configurations containing the supported tokens and their details
- added scalafmt and scalastyle sbt plugins

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
