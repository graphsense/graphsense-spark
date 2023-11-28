package org.graphsense.account.trx

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_unixtime, max}
import org.graphsense.Job
import org.graphsense.account.AccountSink
import org.graphsense.account.config.AccountConfig
import org.graphsense.account.models.{
  AddressIdByAddressPrefix,
  AddressIncomingRelationSecondaryIds,
  AddressOutgoingRelationSecondaryIds,
  AddressTransactionSecondaryIds,
  Block,
  TokenTransfer,
  Transaction,
  TransactionIdByTransactionIdGroup,
  TransactionIdByTransactionPrefix
}
import org.graphsense.TransformHelpers
import org.graphsense.models.ExchangeRates
import org.graphsense.account.trx.models.Trace
import org.graphsense.account.models.TokenConfiguration

class TronJob(
    spark: SparkSession,
    source: TrxSource,
    sink: AccountSink,
    config: AccountConfig
) extends Job {
  import spark.implicits._

  private val transformation = new TrxTransformation(spark, config.bucketSize())

  def prepareAndLoad(): (
      Dataset[ExchangeRates],
      Dataset[Block],
      Dataset[Transaction],
      Dataset[Trace],
      Dataset[TokenTransfer],
      Dataset[TokenConfiguration],
      Long,
      Long,
      Int
  ) = {
    val exchangeRatesRaw = source.exchangeRates()
    val blocks = source.blocks()
    val tokenConfigurations = source.tokenConfigurations().persist()

    println("Store configuration")
    val configuration =
      transformation.configuration(
        config.targetKeyspace(),
        config.bucketSize(),
        config.txPrefixLength(),
        config.addressPrefixLength(),
        TransformHelpers.getFiatCurrencies(exchangeRatesRaw)
      )

    sink.saveConfiguration(configuration)

    println("Store token configuration")
    sink.saveTokenConfiguration(tokenConfigurations)

    println("Computing exchange rates")
    val exchangeRates =
      transformation
        .computeExchangeRates(blocks, exchangeRatesRaw)
        .persist()

    sink.saveExchangeRates(exchangeRates)

    val maxBlockExchangeRates =
      exchangeRates.select(max(col("blockId"))).first.getInt(0)
    val blocksFiltered =
      blocks.filter(col("blockId") <= maxBlockExchangeRates).persist()
    val transactionsFiltered =
      source
        .transactions()
        .filter(col("blockId") <= maxBlockExchangeRates)
        .persist()
    val tracesFiltered =
      source.traces().filter(col("blockId") <= maxBlockExchangeRates).persist()
    val tokenTransfersFiltered = source
      .tokenTransfers()
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

    (
      exchangeRates,
      blocksFiltered,
      transactionsFiltered,
      tracesFiltered,
      tokenTransfersFiltered,
      tokenConfigurations,
      noBlocks,
      noTransactions,
      maxBlockTimestamp
    )
  }

  def computeIntermediateTables(): Unit = {}

  def run(from: Option[Integer], to: Option[Integer]): Unit = {
    println("Running tron specific transformations.")

    val (
      exchangeRates,
      blocks,
      txs,
      traces,
      tokenTxs,
      tokenConfigurations,
      noBlocks,
      noTransactions,
      maxBlockTimestamp
    ) = prepareAndLoad()

    println("Computing address IDs")
    spark.sparkContext.setJobDescription("Computing address IDs")

    val addressIds =
      transformation
        .computeAddressIds(
          traces,
          txs,
          tokenTxs
        )
        .persist()

    val noAddresses = addressIds.count()
    println("no addresses")
    println(noAddresses.toString() + " addresses")

    /* computing and storing balances */
    {
      println("Computing balances")
      val balances = transformation
        .computeBalances(
          blocks,
          txs,
          traces,
          addressIds,
          tokenTxs,
          tokenConfigurations
        )
        .persist()

      println("null balances")
      balances.filter(col("addressId").isNull).show(100)

      sink.saveBalances(balances.filter(col("addressId").isNotNull))
      println("Number of balances: " + balances.count())
      balances.unpersist()
    }

    /* computing and storing address id prefixes */
    println("Computing and storing addr id lookups")

    {
      val addressIdsByAddressPrefix =
        addressIds.toDF.transform(
          TransformHelpers.withSortedPrefix[AddressIdByAddressPrefix](
            "address",
            "addressPrefix",
            config.addressPrefixLength()
          )
        )

      println("null addressIdsByAddressPrefix addresses")
      addressIdsByAddressPrefix.filter(col("addressPrefix").isNull).show(10)

      sink.saveAddressIdsByPrefix(
        addressIdsByAddressPrefix
      )
    }

    println("Computing transaction IDs")
    spark.sparkContext.setJobDescription("Computing transaction IDs")
    val transactionIds =
      transformation.computeTransactionIds(txs).persist()

    /* computing and storing address id prefixes */
    println("Computing and storing txid lookups")

    {
      val transactionIdsByTransactionIdGroup =
        transactionIds.toDF.transform(
          TransformHelpers.withSortedIdGroup[TransactionIdByTransactionIdGroup](
            "transactionId",
            "transactionIdGroup",
            config.bucketSize()
          )
        )

      sink.saveTransactionIdsByGroup(transactionIdsByTransactionIdGroup)

      val transactionIdsByTransactionPrefix =
        transactionIds.toDF.transform(
          TransformHelpers.withSortedPrefix[TransactionIdByTransactionPrefix](
            "transaction",
            "transactionPrefix",
            config.txPrefixLength()
          )
        )
      sink.saveTransactionIdsByTxPrefix(transactionIdsByTransactionPrefix)
    }

    println("Encoding transactions")
    spark.sparkContext.setJobDescription("Encoding transactions")
    val encodedTransactions =
      transformation
        .computeEncodedTransactions(
          traces,
          transactionIds,
          txs,
          addressIds,
          exchangeRates
        )
        .persist()

    txs.unpersist()

    // encodedTransactions.filter(col("transactionId").isNull).show(1000)

    val encodedTokenTransfers = transformation
      .computeEncodedTokenTransfers(
        tokenTxs,
        tokenConfigurations,
        transactionIds,
        addressIds,
        exchangeRates
      )
      .persist()

    exchangeRates.unpersist()
    tokenTxs.unpersist()

    println("Computing and storing block transactions")
    /* computing and storing block txs */

    {
      spark.sparkContext.setJobDescription("Computing block transactions")
      val blockTransactions = transformation
        .computeBlockTransactions(blocks, encodedTransactions)

      sink.saveBlockTransactions(blockTransactions)
    }

    println("Computing and storing address transactions")
    spark.sparkContext.setJobDescription("Computing address transactions")
    val addressTransactions = transformation
      .computeAddressTransactions(encodedTransactions, encodedTokenTransfers)
      .persist()

    {
      sink.saveAddressTransactions(addressTransactions)

      val addressTransactionsSecondaryIds =
        TransformHelpers
          .computeSecondaryPartitionIdLookup[AddressTransactionSecondaryIds](
            addressTransactions.toDF,
            "addressIdGroup",
            "addressIdSecondaryGroup"
          )

      sink.saveAddressTransactionBySecondaryId(addressTransactionsSecondaryIds)
    }

    println("Computing contracts")
    spark.sparkContext.setJobDescription("Computing contracts")
    val contracts = transformation.computeContracts(traces, addressIds)
    traces.unpersist()

    {
      println("Computing address statistics")
      spark.sparkContext.setJobDescription("Computing address statistics")
      val addresses = transformation.computeAddresses(
        encodedTransactions,
        encodedTokenTransfers,
        addressTransactions,
        addressIds,
        contracts
      )

      sink.saveAddresses(addresses)
    }

    val noAddressRelations = {
      println("Computing address relations")
      spark.sparkContext.setJobDescription("Computing address relations")
      val addressRelations =
        transformation
          .computeAddressRelations(
            encodedTransactions,
            encodedTokenTransfers
          )
          .persist()

      sink.saveAddressIncomingRelations(
        addressRelations.sort("dstAddressIdGroup", "dstAddressIdSecondaryGroup")
      )
      sink.saveAddressOutgoingRelations(
        addressRelations.sort("srcAddressIdGroup", "srcAddressIdSecondaryGroup")
      )

      val addressIncomingRelationsSecondaryIds =
        TransformHelpers
          .computeSecondaryPartitionIdLookup[
            AddressIncomingRelationSecondaryIds
          ](
            addressRelations.toDF,
            "dstAddressIdGroup",
            "dstAddressIdSecondaryGroup"
          )
      val addressOutgoingRelationsSecondaryIds =
        TransformHelpers
          .computeSecondaryPartitionIdLookup[
            AddressOutgoingRelationSecondaryIds
          ](
            addressRelations.toDF,
            "srcAddressIdGroup",
            "srcAddressIdSecondaryGroup"
          )

      sink.saveAddressIncomingRelationsBySecondaryId(
        addressIncomingRelationsSecondaryIds
      )
      sink.saveAddressOutgoingRelationsBySecondaryId(
        addressOutgoingRelationsSecondaryIds
      )

      addressRelations.count()
    }

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

    sink.saveSummaryStatistics(summaryStatistics)

  }

}
