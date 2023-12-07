package org.graphsense.account.trx

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.functions.{col, from_unixtime, max, min}
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

  private val debug = config.debug()

  val base_path =
    config.cacheDirectory.toOption.map(dir => dir + config.targetKeyspace())

  def computeCached[
      R: Encoder
  ](
      dataset_name: String
  )(block: => Dataset[R]): Dataset[R] = {
    TransformHelpers.computeCached(base_path, spark)(dataset_name)(block)
  }

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

    val minBlockToProcess = config.minBlock.toOption match {
      case None => exchangeRates.select(min(col("blockId"))).first.getInt(0)
      case Some(user_min_block) =>
        scala.math.max(
          exchangeRates.select(min(col("blockId"))).first.getInt(0),
          user_min_block
        )
    }

    val maxBlockToProcess = config.maxBlock.toOption match {
      case None => exchangeRates.select(max(col("blockId"))).first.getInt(0)
      case Some(user_max_block) =>
        scala.math.min(
          exchangeRates.select(max(col("blockId"))).first.getInt(0),
          user_max_block
        )
    }

    val (
      blks,
      txs,
      traces,
      tokenTxs,
      noBlocks,
      noTransactions,
      maxBlockTimestamp
    ) = time(
      s"Filter source data from ${minBlockToProcess} to ${maxBlockToProcess}"
    ) {

      val blocksFiltered =
        blocks
          .filter(
            col("blockId") >= minBlockToProcess && col(
              "blockId"
            ) <= maxBlockToProcess
          )
          .persist()

      val txsFiltered = source
        .transactions()
        .filter(
          col("blockId") >= minBlockToProcess && col(
            "blockId"
          ) <= maxBlockToProcess
        )
        .persist()

      val minBlock = blocksFiltered
        .select(
          min(col("blockId")).as("minBlockId"),
          min(col("timestamp")).as("minBlockTimestamp")
        )
        .withColumn("minBlockDatetime", from_unixtime(col("minBlockTimestamp")))
      minBlock.select(col("minBlockTimestamp")).first.getInt(0)
      val minBlockDatetime =
        minBlock.select(col("minBlockDatetime")).first.getString(0)

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

      val noBlocks = blocksFiltered.count()
      val noTransactions = txsFiltered.count()

      printStat("Min block timestamp", minBlockDatetime)
      printStat("Min block ID", minBlockToProcess)
      printStat("Max block timestamp", maxBlockDatetime)
      printStat("Max block ID", maxBlockToProcess)
      printStat("Transaction count", noTransactions)
      (
        blocksFiltered,
        txsFiltered,
        source
          .traces()
          .filter(
            col("blockId") >= minBlockToProcess && col(
              "blockId"
            ) <= maxBlockToProcess
          )
          .persist(),
        source
          .tokenTransfers()
          .filter(
            col("blockId") >= minBlockToProcess && col(
              "blockId"
            ) <= maxBlockToProcess
          )
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

    println(txFees.storageLevel.useDisk)
    println(txFees.storageLevel.useMemory)

    val (addressIds, noAddresses) =
      time("Computing Address Ids") {
        spark.sparkContext.setJobDescription("Computing address IDs")

        val ids = computeCached("addressIds") {
          transformation
            .computeAddressIds(
              traces,
              txs,
              tokenTxs
            )
        }

        val noAddresses = ids.count()
        printStat("#addresses", noAddresses.toString())

        printDatasetStats(ids, "addressIds")

        (ids, noAddresses)
      }
    printDatasetStats(addressIds, "addressIds")
    printStat("#addresses", noAddresses)

    /* computing and storing balances */
    time("Computing balances") {
      spark.sparkContext.setJobDescription("Computing balances")
      /* val balances = transformation
         .computeBalancesWithFeesTable(
           blocks,
           txs,
           txFees,
           traces,
           addressIds,
           tokenTxs,
           tokenConfigurations
         )
         .persist()*/

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

      printDatasetStats(balances, "balances")

      if (debug > 0) {
        println("null balances")
        balances.filter(col("addressId").isNull).show(100)
        printStat("#balances", balances.count())
      }

      sink.saveBalances(balances.filter(col("addressId").isNotNull))

      balances.unpersist(true)
      txFees.unpersist(true)
    }

    /* computing and storing address id prefixes */
    time("Computing and storing addr id lookups") {
      spark.sparkContext.setJobDescription(
        "Computing and storing addr id lookups"
      )
      val addressIdsByAddressPrefix =
        addressIds.toDF.transform(
          TransformHelpers.withSortedPrefix[AddressIdByAddressPrefix](
            "address",
            "addressPrefix",
            config.addressPrefixLength()
          )
        )

      printDatasetStats(addressIdsByAddressPrefix, "addressIdsByAddressPrefix")

      if (debug > 0) {
        println("null addressIdsByAddressPrefix addresses")
        addressIdsByAddressPrefix.filter(col("addressPrefix").isNull).show(10)
      }

      sink.saveAddressIdsByPrefix(
        addressIdsByAddressPrefix
      )
    }

    val transactionIds = time("Computing transaction IDs") {
      spark.sparkContext.setJobDescription("Computing transaction IDs")
      computeCached("transactionIds") {
        transformation.computeTransactionIds(txs)
      }
    }

    if (debug > 1) {
      val txs = transactionIds.count()
      val blub =
        transactionIds.dropDuplicates("transaction", "transactionId").count()
      printStat("nr txid", txs)
      printStat("nr txid without duplicates", blub)
    }

    /* computing and storing address id prefixes */
    time("Computing and storing txid lookups") {
      spark.sparkContext.setJobDescription("Computing and storing txid lookups")
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

    val contracts = time("Computing contracts") {
      spark.sparkContext.setJobDescription("Computing contracts")
      computeCached("contracts") {
        transformation.computeContracts(traces, addressIds)
      }
    }
    printDatasetStats(contracts, "contracts")

    if (debug > 0) {
      printStat("# contracts", contracts.count())
    }

    val encodedTransactions = time("Encoding transactions") {
      spark.sparkContext.setJobDescription("Encoding transactions")

      computeCached("encodedTransactions") {
        transformation
          .computeEncodedTransactions(
            traces,
            transactionIds,
            txs,
            addressIds,
            exchangeRates
          )
      }
    }

    printDatasetStats(encodedTransactions, "encodedTransactions")

    /*
      Caution this is a functional count it allows
      to unpersist tx and traces here since
      dataset is forced to be cached
     */
    printStat("#encoded Txs", encodedTransactions.count())
    txs.unpersist(true)
    traces.unpersist(true)

    val encodedTokenTransfers =
      time("Compute encoded token txs") {
        spark.sparkContext.setJobDescription("Compute encoded token txs")
        computeCached("encodedTokenTransfers") {
          transformation
            .computeEncodedTokenTransfers(
              tokenTxs,
              tokenConfigurations,
              transactionIds,
              addressIds,
              exchangeRates
            )
        }
      }

    printDatasetStats(encodedTokenTransfers, "encodedTokenTransfers")

    /*
      Caution this is a functional count it allows
      to unpersist exchangeRates, txids, tokentxs here since
      dataset is forced to be cached
     */
    printStat("#encoded token Txs", encodedTokenTransfers.count())

    exchangeRates.unpersist(true)
    transactionIds.unpersist(true)
    tokenTxs.unpersist(true)

    time("Computing and storing block transactions") {
      spark.sparkContext.setJobDescription(
        "Computing and storing block transactions"
      )
      val blockTransactions =
        computeCached("blockTransactions") {
          transformation
            .computeBlockTransactions(blocks, encodedTransactions)
        }

      printDatasetStats(blockTransactions, "blockTransactions")
      sink.saveBlockTransactionsRelational(blockTransactions)
      blockTransactions.unpersist()
    }

    val addressTransactions = time("Computing address transactions") {
      spark.sparkContext.setJobDescription("Computing address transactions")
      computeCached("addressTransactions") {
        transformation
          .computeAddressTransactions(
            encodedTransactions,
            encodedTokenTransfers
          )
      }
    }
    printDatasetStats(addressTransactions, "addressTransactions")

    if (debug > 0) {
      printStat("#address Txs", addressTransactions.count())
    }

    time("Saving address transactions and lookups") {
      spark.sparkContext.setJobDescription(
        "Saving address transactions and lookups"
      )
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

    time("Computing address and storing statistics") {
      spark.sparkContext.setJobDescription(
        "Computing address and storing statistics"
      )
      val addresses =
        computeCached("addresses") {
          transformation.computeAddresses(
            encodedTransactions,
            encodedTokenTransfers,
            addressTransactions,
            addressIds,
            contracts
          )
        }

      printDatasetStats(addresses, "addresses")

      sink.saveAddresses(addresses)
      addresses.unpersist(true)
    }

    addressTransactions.unpersist(true)
    addressIds.unpersist(true)
    contracts.unpersist(true)

    val noAddressRelations = time("Computing and storing address relations") {
      spark.sparkContext.setJobDescription(
        "Computing and storing address relations"
      )
      val addressRelations =
        computeCached("addressRelations") {
          transformation
            .computeAddressRelations(
              encodedTransactions,
              encodedTokenTransfers
            )
        }

      printDatasetStats(addressRelations, "addressRelations")

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
