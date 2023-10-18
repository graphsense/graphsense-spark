package org.graphsense

import com.datastax.spark.connector.ColumnName
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_unixtime, max}
import org.graphsense.storage.CassandraStorage
import org.rogach.scallop._

object TransformationJob {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
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
    verify()
  }

  def main(args: Array[String]) {

    val conf = new Conf(args)

    val spark = SparkSession.builder
      .appName("GraphSense Transformation [%s]".format(conf.targetKeyspace()))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    println("Raw keyspace:                  " + conf.rawKeyspace())
    println("Target keyspace:               " + conf.targetKeyspace())
    println("Bucket size:                   " + conf.bucketSize())
    println("Address prefix length:         " + conf.addressPrefixLength())
    println("Tx prefix length:              " + conf.txPrefixLength())

    import spark.implicits._

    val cassandra = new CassandraStorage(spark)

    val exchangeRatesRaw =
      cassandra.load[ExchangeRatesRaw](conf.rawKeyspace(), "exchange_rates")

    val blocks =
      cassandra.load[Block](conf.rawKeyspace(), "block")
    val transactions =
      cassandra.load[Transaction](conf.rawKeyspace(), "transaction")
    val traces = cassandra.load[Trace](
      conf.rawKeyspace(),
      "trace",
      Array(
        "block_id_group",
        "block_id",
        "trace_id",
        "trace_index",
        "from_address",
        "to_address",
        "value",
        "status",
        "call_type",
        "tx_hash"
      ).map(
        ColumnName(_)
      ): _*
    )

    val tt = new TokenTransfers(spark)
    // Transfer(address,address,uint256)
    val tokenConfigurations = tt.getTokenConfigurations().persist()
    val tokenTransfers = tt
      .getTokenTransfers(
        cassandra
          .load[Log](
            conf.rawKeyspace(),
            "log"
          ),
        tt.tokenAddresses
      )

    val transformation = new Transformation(spark, conf.bucketSize())

    println("Store configuration")
    val configuration =
      transformation.configuration(
        conf.targetKeyspace(),
        conf.bucketSize(),
        conf.txPrefixLength(),
        conf.addressPrefixLength(),
        transformation.getFiatCurrencies(exchangeRatesRaw)
      )
    cassandra.store(
      conf.targetKeyspace(),
      "configuration",
      configuration
    )

    println("Store token configuration")
    cassandra.store(
      conf.targetKeyspace(),
      "token_configuration",
      tokenConfigurations
    )

    println("Computing exchange rates")
    val exchangeRates =
      transformation
        .computeExchangeRates(blocks, exchangeRatesRaw)
        .persist()
    cassandra.store(conf.targetKeyspace(), "exchange_rates", exchangeRates)

    val maxBlockExchangeRates =
      exchangeRates.select(max(col("blockId"))).first.getInt(0)
    val blocksFiltered =
      blocks.filter(col("blockId") <= maxBlockExchangeRates).persist()
    val transactionsFiltered =
      transactions.filter(col("blockId") <= maxBlockExchangeRates).persist()
    val tracesFiltered =
      traces.filter(col("blockId") <= maxBlockExchangeRates).persist()
    val tokenTransfersFiltered = tokenTransfers
      .filter(col("blockId") <= maxBlockExchangeRates)
      .persist()

    val maxBlock = blocksFiltered
      .select(
        max(col("blockId")).as("maxBlockId"),
        max(col("timestamp")).as("maxBlockTimestamp")
      )
      .withColumn("maxBlockDatetime", from_unixtime(col("maxBlockTimestamp")))
    val maxBlockTimestamp =
      maxBlock.select(col("maxBlockTimestamp")).first.getInt(0)
    val maxBlockDatetime =
      maxBlock.select(col("maxBlockDatetime")).first.getString(0)

    val noBlocks = maxBlockExchangeRates.toLong + 1
    val noTransactions = transactionsFiltered.count()

    println(s"Max block timestamp: ${maxBlockDatetime}")
    println(s"Max block ID: ${maxBlockExchangeRates}")
    println(s"Max transaction ID: ${noTransactions - 1}")

    println("Computing transaction IDs")
    spark.sparkContext.setJobDescription("Computing transaction IDs")
    val transactionIds =
      transformation.computeTransactionIds(transactionsFiltered).persist()
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
          "transactionPrefix",
          conf.txPrefixLength()
        )
      )
    cassandra.store(
      conf.targetKeyspace(),
      "transaction_ids_by_transaction_prefix",
      transactionIdsByTransactionPrefix
    )

    println("Computing address IDs")
    spark.sparkContext.setJobDescription("Computing address IDs")
    val addressIds =
      transformation
        .computeAddressIds(tracesFiltered, tokenTransfersFiltered)
        .persist()
    val noAddresses = addressIds.count()
    val addressIdsByAddressPrefix =
      addressIds.toDF.transform(
        transformation.withSortedPrefix[AddressIdByAddressPrefix](
          "address",
          "addressPrefix",
          conf.addressPrefixLength()
        )
      )
    cassandra.store(
      conf.targetKeyspace(),
      "address_ids_by_address_prefix",
      addressIdsByAddressPrefix
    )

    println("Computing contracts")
    spark.sparkContext.setJobDescription("Computing contracts")
    val contracts = transformation.computeContracts(tracesFiltered, addressIds)

    println("Computing balances")

    val balances = transformation
      .computeBalances(
        blocksFiltered,
        transactionsFiltered,
        tracesFiltered,
        addressIds,
        tokenTransfersFiltered,
        tokenConfigurations
      )
      .persist()
    cassandra.store(conf.targetKeyspace(), "balance", balances)
    println("Number of balances: " + balances.count())

    println("Encoding transactions")
    spark.sparkContext.setJobDescription("Encoding transactions")
    val encodedTransactions =
      transformation
        .computeEncodedTransactions(
          tracesFiltered,
          transactionIds,
          addressIds,
          exchangeRates
        )
        .persist()

    val encodedTokenTransfers = transformation
      .computeEncodedTokenTransfers(
        tokenTransfersFiltered,
        tokenConfigurations,
        transactionIds,
        addressIds,
        exchangeRates
      )
      .persist()

    println("Computing block transactions")
    spark.sparkContext.setJobDescription("Computing block transactions")
    val blockTransactions = transformation
      .computeBlockTransactions(blocksFiltered, encodedTransactions)
    cassandra.store(
      conf.targetKeyspace(),
      "block_transactions",
      blockTransactions
    )

    println("Computing address transactions")
    spark.sparkContext.setJobDescription("Computing address transactions")
    val addressTransactions = transformation
      .computeAddressTransactions(encodedTransactions, encodedTokenTransfers)
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

    println("Computing address statistics")
    spark.sparkContext.setJobDescription("Computing address statistics")
    val addresses = transformation.computeAddresses(
      encodedTransactions,
      encodedTokenTransfers,
      addressTransactions,
      addressIds,
      contracts
    )
    cassandra.store(conf.targetKeyspace(), "address", addresses)

    println("Computing address relations")
    spark.sparkContext.setJobDescription("Computing address relations")
    val addressRelations =
      transformation.computeAddressRelations(
        encodedTransactions,
        encodedTokenTransfers
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
    spark.sparkContext.setJobDescription("Computing summary statistics")
    val summaryStatistics =
      transformation.summaryStatistics(
        maxBlockTimestamp,
        noBlocks,
        noTransactions,
        noAddresses,
        noAddressRelations
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
