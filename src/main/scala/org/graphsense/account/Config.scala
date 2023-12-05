package org.graphsense.account.config

import org.rogach.scallop._

class AccountConfig(arguments: Seq[String]) extends ScallopConf(arguments) {
  val rawKeyspace: ScallopOption[String] =
    opt[String](
      "raw-keyspace",
      required = true,
      noshort = true,
      descr = "Raw keyspace"
    )
  val targetKeyspace: ScallopOption[String] = opt[String](
    "target-keyspace",
    required = true,
    noshort = true,
    descr = "Transformed keyspace"
  )
  val bucketSize: ScallopOption[Int] = opt[Int](
    "bucket-size",
    required = false,
    default = Some(25000),
    noshort = true,
    descr = "Bucket size for Cassandra partitions"
  )
  val addressPrefixLength: ScallopOption[Int] = opt[Int](
    "address-prefix-length",
    required = false,
    default = Some(5),
    noshort = true,
    descr = "Prefix length of address hashes for Cassandra partitioning keys"
  )
  val txPrefixLength: ScallopOption[Int] = opt[Int](
    "tx-prefix-length",
    required = false,
    default = Some(5),
    noshort = true,
    descr = "Prefix length for tx hashes Cassandra partitioning keys"
  )
  val network: ScallopOption[String] = opt[String](
    "network",
    required = true,
    noshort = true,
    descr =
      "Select which network we are processing (supported at the moment are, eth, trx)"
  )
  val minBlock: ScallopOption[Int] = opt[Int](
    "min-block",
    required = false,
    default = None,
    noshort = true,
    descr = "Earliest block to process."
  )
  val maxBlock: ScallopOption[Int] = opt[Int](
    "max-block",
    required = false,
    default = None,
    noshort = true,
    descr = "Limit the max block to process"
  )
  verify()
}
