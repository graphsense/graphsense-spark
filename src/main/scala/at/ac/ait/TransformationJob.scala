package at.ac.ait

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lower, max}
import org.rogach.scallop._

import at.ac.ait.storage._

object TransformationJob {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val rawKeyspace: ScallopOption[String] =
      opt[String](
        "raw-keyspace",
        required = true,
        noshort = true,
        descr = "Raw keyspace"
      )
    val tagKeyspace: ScallopOption[String] =
      opt[String](
        "tag-keyspace",
        required = true,
        noshort = true,
        descr = "Tag keyspace"
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
    verify()
  }

  def main(args: Array[String]) {

    val conf = new Conf(args)

    val spark = SparkSession.builder
      .appName("GraphSense Transformation [%s]".format(conf.targetKeyspace()))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    println("Raw keyspace:                  " + conf.rawKeyspace())
    println("Tag keyspace:                  " + conf.tagKeyspace())
    println("Target keyspace:               " + conf.targetKeyspace())
    println("Bucket size:                   " + conf.bucketSize())

    import spark.implicits._

    val cassandra = new CassandraStorage(spark)

    val exchangeRatesRaw =
      cassandra.load[ExchangeRatesRaw](conf.rawKeyspace(), "exchange_rates")

    val blocks =
      cassandra.load[Block](conf.rawKeyspace(), "block")
    val transactions =
      cassandra.load[Transaction](conf.rawKeyspace(), "transaction")
    val tagsRaw = cassandra
      .load[TagRaw](conf.tagKeyspace(), "tag_by_address")

    val transformation = new Transformation(spark, conf.bucketSize())

    println("Store configuration")
    val configuration =
      transformation.configuration(
        conf.targetKeyspace(),
        conf.bucketSize(),
        transformation.getFiatCurrencies(exchangeRatesRaw)
      )
    cassandra.store(
      conf.targetKeyspace(),
      "configuration",
      configuration
    )

    val noBlocks = blocks.count()
    println("Number of blocks: " + noBlocks)
    val lastBlockTimestamp = blocks
      .select(max(col("timestamp")))
      .first()
      .getInt(0)
    val noTransactions = transactions.count()
    println("Number of transactions: " + noTransactions)

    println("Computing exchange rates")
    val exchangeRates =
      transformation
        .computeExchangeRates(blocks, exchangeRatesRaw)
        .persist()
    cassandra.store(conf.targetKeyspace(), "exchange_rates", exchangeRates)

    println("Computing transaction IDs")
    val transactionIds =
      transformation.computeTransactionIds(transactions).persist()
    val transactionIdsByTransactionIdGroup =
      transactionIds.toDF.transform(
        transformation.withSortedIdGroup[TransactionIdByTransactionIdGroup](
          "transactionId",
          "transactionIdGroup"
        )
      )
    cassandra.store(
      conf.targetKeyspace(),
      "transaction_ids_by_transaction_id_group",
      transactionIdsByTransactionIdGroup
    )
    val transactionIdsByTransactionPrefix =
      transactionIds.toDF.transform(
        transformation.withSortedPrefix[TransactionIdByTransactionPrefix](
          "transaction",
          "transactionPrefix"
        )
      )
    cassandra.store(
      conf.targetKeyspace(),
      "transaction_ids_by_transaction_prefix",
      transactionIdsByTransactionPrefix
    )

    println("Computing address IDs")
    val addressIds = transformation.computeAddressIds(transactions)
    val noAddresses = addressIds.count()
    val addressIdsByAddressIdGroup =
      addressIds.toDF.transform(
        transformation.withSortedIdGroup[AddressIdByAddressIdGroup](
          "addressId",
          "addressIdGroup"
        )
      )
    cassandra.store(
      conf.targetKeyspace(),
      "address_ids_by_address_id_group",
      addressIdsByAddressIdGroup
    )
    val addressIdsByAddressPrefix =
      addressIds.toDF.transform(
        transformation.withSortedPrefix[AddressIdByAddressPrefix](
          "address",
          "addressPrefix"
        )
      )
    cassandra.store(
      conf.targetKeyspace(),
      "address_ids_by_address_prefix",
      addressIdsByAddressPrefix
    )

    println("Encoding transactions")
    val encodedTransactions =
      transformation
        .computeEncodedTransactions(
          transactions,
          transactionIds,
          addressIds,
          exchangeRates
        )
        .persist()

    println("Computing block transactions")
    val blockTransactions = transformation
      .computeBlockTransactions(blocks, encodedTransactions)
    cassandra.store(
      conf.targetKeyspace(),
      "block_transactions",
      blockTransactions
    )

    println("Computing address transactions")
    val addressTransactions = transformation
      .computeAddressTransactions(encodedTransactions)
      .persist()
    cassandra.store(
      conf.targetKeyspace(),
      "address_transactions",
      addressTransactions
    )
    val addressTransactionsSecondaryIds =
      transformation
        .computeSecondaryPartitionIdLookup[AddressTransactionSecondaryIds](
          addressTransactions.toDF,
          "addressIdGroup",
          "addressIdSecondaryGroup"
        )
    cassandra.store(
      conf.targetKeyspace(),
      "address_transactions_secondary_ids",
      addressTransactionsSecondaryIds
    )

    println("Computing address tags")
    val addressTags =
      transformation
        .computeAddressTags(
          tagsRaw,
          addressIds,
          "ETH"
        )
        .persist()
    cassandra.store(conf.targetKeyspace(), "address_tags", addressTags)
    val noAddressTags = addressTags
      .select(col("label"))
      .withColumn("label", lower(col("label")))
      .distinct()
      .count()

    println("Computing address statistics")
    val addresses = transformation.computeAddresses(
      encodedTransactions,
      addressTransactions
    )
    cassandra.store(conf.targetKeyspace(), "address", addresses)

    println("Computing address relations")
    val addressRelations =
      transformation.computeAddressRelations(
        encodedTransactions,
        addresses
      )
    val noAddressRelations = addressRelations.count()

    cassandra.store(
      conf.targetKeyspace(),
      "address_incoming_relations",
      addressRelations.sort("dstAddressIdGroup", "dstAddressIdSecondaryGroup")
    )
    cassandra.store(
      conf.targetKeyspace(),
      "address_outgoing_relations",
      addressRelations.sort("srcAddressIdGroup", "srcAddressIdSecondaryGroup")
    )

    val addressIncomingRelationsSecondaryIds =
      transformation
        .computeSecondaryPartitionIdLookup[AddressIncomingRelationSecondaryIds](
          addressRelations.toDF,
          "dstAddressIdGroup",
          "dstAddressIdSecondaryGroup"
        )
    val addressOutgoingRelationsSecondaryIds =
      transformation
        .computeSecondaryPartitionIdLookup[AddressOutgoingRelationSecondaryIds](
          addressRelations.toDF,
          "srcAddressIdGroup",
          "srcAddressIdSecondaryGroup"
        )

    cassandra.store(
      conf.targetKeyspace(),
      "address_incoming_relations_secondary_ids",
      addressIncomingRelationsSecondaryIds
    )
    cassandra.store(
      conf.targetKeyspace(),
      "address_outgoing_relations_secondary_ids",
      addressOutgoingRelationsSecondaryIds
    )

    println("Computing summary statistics")
    val summaryStatistics =
      transformation.summaryStatistics(
        lastBlockTimestamp,
        noBlocks,
        noTransactions,
        noAddresses,
        noAddressRelations,
        noAddressTags
      )
    summaryStatistics.show()
    cassandra.store(
      conf.targetKeyspace(),
      "summary_statistics",
      summaryStatistics
    )

    spark.stop()
  }
}
