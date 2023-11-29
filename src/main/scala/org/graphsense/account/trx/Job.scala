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
import org.graphsense.account.trx.models.{Trace, TxFee}
import org.graphsense.account.models.TokenConfiguration
import org.graphsense.Util._

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
      Dataset[TxFee],
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

    time("Store configuration") {
      val conf = transformation.configuration(
        config.targetKeyspace(),
        config.bucketSize(),
        config.txPrefixLength(),
        config.addressPrefixLength(),
        TransformHelpers.getFiatCurrencies(exchangeRatesRaw)
      )

      sink.saveConfiguration(conf)
    }

    time("Store token configuration") {
      sink.saveTokenConfiguration(tokenConfigurations)
    }

    val exchangeRates = time("Computing exchange rates") {
      val rates =
        transformation
          .computeExchangeRates(blocks, exchangeRatesRaw)
          .persist()

      sink.saveExchangeRates(rates)
      rates
    }

    val maxBlockExchangeRates =
      exchangeRates.select(max(col("blockId"))).first.getInt(0)

    val (
      blks,
      txs,
      traces,
      tokenTxs,
      noBlocks,
      noTransactions,
      maxBlockTimestamp
    ) = time(s"Filter source data above ${maxBlockExchangeRates}") {

      val blocksFiltered =
        blocks.filter(col("blockId") <= maxBlockExchangeRates).persist()

      val txsFiltred = source
        .transactions()
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
      val noTransactions = txsFiltred.count()

      printStat("Max block timestamp", maxBlockDatetime)
      printStat("Max block ID", maxBlockExchangeRates)
      printStat("Max transaction ID", noTransactions - 1)
      (
        blocksFiltered,
        txsFiltred,
        source
          .traces()
          .filter(col("blockId") <= maxBlockExchangeRates)
          .persist(),
        source
          .tokenTransfers()
          .filter(col("blockId") <= maxBlockExchangeRates)
          .persist(),
        noBlocks,
        noTransactions,
        maxBlockTimestamp
      )
    }

    (
      exchangeRates,
      blks,
      txs,
      source.txFee(), // For now unfiltered.
      traces,
      tokenTxs,
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
      txFees,
      traces,
      tokenTxs,
      tokenConfigurations,
      noBlocks,
      noTransactions,
      maxBlockTimestamp
    ) = prepareAndLoad()

    val (addressIds, noAddresses) =
      time("Computing Address Ids") {
        spark.sparkContext.setJobDescription("Computing address IDs")
        val ids = transformation
          .computeAddressIds(
            traces,
            txs,
            tokenTxs
          )
          .persist()
        val noAddresses = ids.count()
        printStat("#addresses", noAddresses.toString())
        (ids, noAddresses)
      }

    /* computing and storing balances */
    time("Computing balances") {
      val balances = transformation
        .computeBalancesWithFeesTable(
          blocks,
          txs,
          txFees,
          traces,
          addressIds,
          tokenTxs,
          tokenConfigurations
        )
        .persist()

      println("null balances")
      balances.filter(col("addressId").isNull).show(100)

      sink.saveBalances(balances.filter(col("addressId").isNotNull))
      printStat("#balances", balances.count())
      // balances.unpersist()
    }

    /* computing and storing address id prefixes */
    time("Computing and storing addr id lookups") {
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

    val transactionIds = time("Computing transaction IDs") {
      spark.sparkContext.setJobDescription("Computing transaction IDs")
      transformation.computeTransactionIds(txs).persist()
    }

    /* computing and storing address id prefixes */
    time("Computing and storing txid lookups") {
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

    val encodedTransactions = time("Encoding transactions") {
      spark.sparkContext.setJobDescription("Encoding transactions")

      transformation
        .computeEncodedTransactions(
          traces,
          transactionIds,
          txs,
          addressIds,
          exchangeRates
        )
        .persist()
    }
    // txs.unpersist()

    val encodedTokenTransfers =
      time("Compute encoded token txs") {
        transformation
          .computeEncodedTokenTransfers(
            tokenTxs,
            tokenConfigurations,
            transactionIds,
            addressIds,
            exchangeRates
          )
          .persist()
      }

    // exchangeRates.unpersist()
    // tokenTxs.unpersist()

    time("Computing and storing block transactions") {
      spark.sparkContext.setJobDescription("Computing block transactions")
      val blockTransactions = transformation
        .computeBlockTransactions(blocks, encodedTransactions)

      sink.saveBlockTransactions(blockTransactions)
    }

    val addressTransactions = time("Computing address transactions") {
      spark.sparkContext.setJobDescription("Computing address transactions")
      transformation
        .computeAddressTransactions(encodedTransactions, encodedTokenTransfers)
        .persist()
    }

    time("Saving address transactions and lookups") {
      sink.saveAddressTransactions(addressTransactions)

      addressTransactions.show(100)

      val addressTransactionsSecondaryIds =
        TransformHelpers
          .computeSecondaryPartitionIdLookup[AddressTransactionSecondaryIds](
            addressTransactions.toDF,
            "addressIdGroup",
            "addressIdSecondaryGroup"
          )

      sink.saveAddressTransactionBySecondaryId(addressTransactionsSecondaryIds)
    }

    val contracts = time("Computing contracts") {
      spark.sparkContext.setJobDescription("Computing contracts")
      transformation.computeContracts(traces, addressIds)
    }
    // traces.unpersist()

    time("Computing address and storing statistics") {
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

    // addressTransactions.unpersist()
    // addressIds.unpersist()
    // contracts.unpersist()

    val noAddressRelations = time("Computing and storing address relations") {
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

    time("Computing summary statistics") {
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

}
