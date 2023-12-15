package org.graphsense.account.trx

import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{
  broadcast,
  col,
  count,
  lit,
  row_number,
  sum,
  transform,
  when,
  xxhash64
}
import org.apache.spark.sql.types.{DecimalType, FloatType}
import org.graphsense.TransformHelpers
import org.graphsense.account.trx.models._
import org.graphsense.account.models._
import org.graphsense.models.{ExchangeRates, ExchangeRatesRaw}
import org.graphsense.account.eth.EthTransformation
import org.graphsense.Util._

class TrxTransformation(spark: SparkSession, bucketSize: Int) {

  import spark.implicits._

  val ethTransform = new EthTransformation(spark, bucketSize)

  val isSuccessfulTransaction = $"receiptStatus" === 1
  val isSuccessfulTrace: Column = $"rejected" === false
  val isCreationTrace: Column = $"note" === "create" && isSuccessfulTrace
  val isCreationTx: Column =
    $"toAddress".isNull && $"receiptContractAddress".isNotNull
  val isTrxTrace: Column = $"callTokenId".isNull
  val isCallTrace: Column = $"note" === "call"

  def txContractCreationAsToAddress[T](ds: Dataset[T]): DataFrame = {
    ds.withColumn(
      "toAddress",
      when(
        isCreationTx,
        col("receiptContractAddress")
      ).otherwise(col("toAddress"))
    )
  }

  def removeUnknownRecipientTxs[T](ds: Dataset[T]): Dataset[T] = {
    ds.filter($"toAddress".isNotNull && $"receiptContractAddress".isNull)
  }

  def onlySuccessfulTxs[T](ds: Dataset[T]): Dataset[T] = {
    ds.filter(isSuccessfulTransaction)
  }

  def onlySuccessfulTrace[T](ds: Dataset[T]): Dataset[T] = {
    ds.filter(isSuccessfulTrace)
  }

  def onlySuccessfulTrxCallTraces[T](ds: Dataset[T]): Dataset[T] = {
    ds.filter(isTrxTrace && isSuccessfulTrace && isCallTrace)
  }

  def joinAddressIds[T](
      addressIds: Dataset[AddressId],
      addressCol: String = "address"
  )(ds: Dataset[T]): DataFrame = {
    ds.join(
      addressIds.withColumnRenamed("address", addressCol),
      Seq(addressCol),
      "left"
    ).drop(addressCol)
  }

  def joinTransactionIds[T](
      txIds: Dataset[TransactionId],
      txHashCol: String = "txHash"
  )(ds: Dataset[T]): DataFrame = {
    ds.join(
      txIds.withColumnRenamed("transaction", txHashCol),
      Seq(txHashCol),
      "left"
    ).drop(txHashCol)
  }

  def configuration(
      keyspaceName: String,
      bucketSize: Int,
      txPrefixLength: Int,
      addressPrefixLength: Int,
      fiatCurrencies: Seq[String]
  ) = {
    ethTransform.configuration(
      keyspaceName,
      bucketSize,
      txPrefixLength,
      addressPrefixLength,
      fiatCurrencies
    )
  }

  def computeExchangeRates(
      blocks: Dataset[Block],
      exchangeRates: Dataset[ExchangeRatesRaw]
  ): Dataset[ExchangeRates] = {
    ethTransform.computeExchangeRates(blocks, exchangeRates)
  }

  // TODO: remove old compute with balances if this is right.
  def computeBalancesWithFeesTable(
      blocks: Dataset[Block],
      transactions: Dataset[Transaction],
      txFees: Dataset[TxFee],
      traces: Dataset[Trace],
      addressIds: Dataset[AddressId],
      tokenTransfers: Dataset[TokenTransfer],
      tokenConfigurations: Dataset[TokenConfiguration]
  ): Dataset[Balance] = {
    val txs = transactions
      .transform(onlySuccessfulTxs)
      .transform(txContractCreationAsToAddress)
      .transform(removeUnknownRecipientTxs)

    val trcs = traces.transform(onlySuccessfulTrxCallTraces)

    val traceDebits = trcs
      .groupBy("transfertoAddress")
      .agg(sum("callValue").as("traceDebits"))
      .withColumnRenamed("transfertoAddress", "address")
      .transform(joinAddressIds(addressIds))

    val traceCredits = trcs
      .groupBy("callerAddress")
      .agg((-sum($"callValue")).as("traceCredits"))
      .withColumnRenamed("callerAddress", "address")
      .transform(joinAddressIds(addressIds))

    val txDebits = txs
      .groupBy("toAddress")
      .agg(sum("value").as("txDebits"))
      .withColumnRenamed("toAddress", "address")
      .transform(joinAddressIds(addressIds))

    val txCredits = txs
      .groupBy("fromAddress")
      .agg((-sum("value")).as("txCredits"))
      .withColumnRenamed("fromAddress", "address")
      .transform(joinAddressIds(addressIds))

    // TODO: check what miners really get of the fees
    val txFeeDebits = txs
      .join(txFees, Seq("txHash"), "inner")
      .join(blocks, Seq("blockId"), "inner")
      .withColumn("calculatedValue", $"fee")
      .groupBy("miner")
      .agg(sum("calculatedValue").as("txFeeDebits"))
      .withColumnRenamed("miner", "address")
      .transform(joinAddressIds(addressIds))

    // TODO: check if this is really all that is deduced from the sender.
    val txFeeCredits = txs
      .join(txFees, Seq("txHash"), "inner")
      .withColumn("calculatedValue", -col("fee"))
      .groupBy("fromAddress")
      .agg(sum("calculatedValue").as("txFeeCredits"))
      .withColumnRenamed("fromAddress", "address")
      .transform(joinAddressIds(addressIds))

    // TODO: Check if there are burned fees.
    // val burntFees = blocks.na
    //   .fill(0, Seq("baseFeePerGas"))
    //   .withColumn(
    //     "value",
    //     -col("baseFeePerGas").cast(DecimalType(38, 0)) * col("gasUsed")
    //   )
    //   .groupBy("miner")
    //   .agg(sum("value").as("burntFees"))
    //   .withColumnRenamed("miner", "address")
    //   .join(addressIds, Seq("address"), "left")
    //   .drop("address")

    val balance = traceDebits
      .join(traceCredits, Seq("addressId"), "full")
      .join(txFeeDebits, Seq("addressId"), "full")
      .join(txFeeCredits, Seq("addressId"), "full")
      .join(txDebits, Seq("addressId"), "full")
      .join(txCredits, Seq("addressId"), "full")
      .na
      .fill(0)
      .withColumn(
        "balance",
        $"traceDebits" + $"traceCredits" +
          $"txDebits" + $"txCredits" +
          $"txFeeDebits" + $"txFeeCredits"
      )
      .withColumn("currency", lit("TRX"))
      .transform(
        TransformHelpers
          .withSortedIdGroup[Balance]("addressId", "addressIdGroup", bucketSize)
      )
      .select("addressIdGroup", "addressId", "balance", "currency")
      .as[Balance]

    val tokenCredits = tokenTransfers
      .groupBy("from", "tokenAddress")
      .agg((-sum($"value")).as("credits"))
      .withColumnRenamed("from", "address")
      .transform(joinAddressIds(addressIds))

    val tokenDebits = tokenTransfers
      .groupBy("to", "tokenAddress")
      .agg((sum($"value")).as("debits"))
      .withColumnRenamed("to", "address")
      .transform(joinAddressIds(addressIds))

    val balanceTokensTmp = tokenCredits
      .join(tokenDebits, Seq("addressId", "tokenAddress"), "full")
      .na
      .fill(0, Seq("credits", "debits"))
      .withColumn(
        "balance",
        $"debits" + $"credits"
      )
      .join(tokenConfigurations, Seq("tokenAddress"), "left")
      .withColumn("currency", $"currencyTicker")

    val balanceTokens = balanceTokensTmp
      .transform(
        TransformHelpers
          .withSortedIdGroup[Balance]("addressId", "addressIdGroup", bucketSize)
      )
      .select("addressIdGroup", "addressId", "balance", "currency")
      .as[Balance]

    balance.union(balanceTokens)
  }

  def computeBalances(
      blocks: Dataset[Block],
      transactions: Dataset[Transaction],
      traces: Dataset[Trace],
      addressIds: Dataset[AddressId],
      tokenTransfers: Dataset[TokenTransfer],
      tokenConfigurations: Dataset[TokenConfiguration]
  ): Dataset[Balance] = {

    val txs = transactions
      .transform(onlySuccessfulTxs)
      .transform(txContractCreationAsToAddress)
      .transform(removeUnknownRecipientTxs)
    val trcs = traces.transform(onlySuccessfulTrxCallTraces)

    val traceDebits = trcs
      .groupBy("transfertoAddress")
      .agg(sum("callValue").as("traceDebits"))
      .withColumnRenamed("transfertoAddress", "address")
      .transform(joinAddressIds(addressIds))

    val traceCredits = trcs
      .groupBy("callerAddress")
      .agg((-sum($"callValue")).as("traceCredits"))
      .withColumnRenamed("callerAddress", "address")
      .transform(joinAddressIds(addressIds))

    val txDebits = txs
      .groupBy("toAddress")
      .agg(sum("value").as("txDebits"))
      .withColumnRenamed("toAddress", "address")
      .transform(joinAddressIds(addressIds))

    val txCredits = txs
      .groupBy("fromAddress")
      .agg((-sum("value")).as("txCredits"))
      .withColumnRenamed("fromAddress", "address")
      .transform(joinAddressIds(addressIds))

    val txFeeDebits = txs
      .join(blocks, Seq("blockId"), "inner")
      .withColumn("calculatedValue", $"receiptGasUsed" * $"gasPrice")
      .groupBy("miner")
      .agg(sum("calculatedValue").as("txFeeDebits"))
      .withColumnRenamed("miner", "address")
      .transform(joinAddressIds(addressIds))

    val txFeeCredits = txs
      .withColumn("calculatedValue", -col("receiptGasUsed") * $"gasPrice")
      .groupBy("fromAddress")
      .agg(sum("calculatedValue").as("txFeeCredits"))
      .withColumnRenamed("fromAddress", "address")
      .join(addressIds, Seq("address"), "left")
      .drop("address")

    val burntFees = blocks.na
      .fill(0, Seq("baseFeePerGas"))
      .withColumn(
        "value",
        -col("baseFeePerGas").cast(DecimalType(38, 0)) * $"gasUsed"
      )
      .groupBy("miner")
      .agg(sum("value").as("burntFees"))
      .withColumnRenamed("miner", "address")
      .join(addressIds, Seq("address"), "left")
      .drop("address")

    val balance = burntFees
      .join(traceDebits, Seq("addressId"), "full")
      .join(traceCredits, Seq("addressId"), "full")
      .join(txFeeDebits, Seq("addressId"), "full")
      .join(txFeeCredits, Seq("addressId"), "full")
      .join(txDebits, Seq("addressId"), "full")
      .join(txCredits, Seq("addressId"), "full")
      .na
      .fill(0)
      .withColumn(
        "balance",
        $"burntFees" +
          $"traceDebits" + $"traceCredits" +
          $"txDebits" + $"txCredits" +
          $"txFeeDebits" + $"txFeeCredits"
      )
      .withColumn("currency", lit("TRX"))
      .transform(
        TransformHelpers
          .withSortedIdGroup[Balance]("addressId", "addressIdGroup", bucketSize)
      )
      .select("addressIdGroup", "addressId", "balance", "currency")
      .as[Balance]

    val tokenCredits = tokenTransfers
      .groupBy("from", "tokenAddress")
      .agg((-sum("value")).as("credits"))
      .withColumnRenamed("from", "address")
      .join(addressIds, Seq("address"), "left")
      .drop("address")

    val tokenDebits = tokenTransfers
      .groupBy("to", "tokenAddress")
      .agg((sum("value")).as("debits"))
      .withColumnRenamed("to", "address")
      .join(addressIds, Seq("address"), "left")
      .drop("address")

    val balanceTokensTmp = tokenCredits
      .join(tokenDebits, Seq("addressId", "tokenAddress"), "full")
      .na
      .fill(0, Seq("credits", "debits"))
      .withColumn(
        "balance",
        $"debits" + $"credits"
      )
      .join(tokenConfigurations, Seq("tokenAddress"), "left")
      .withColumn("currency", $"currencyTicker")

    val balanceTokens = balanceTokensTmp
      .transform(
        TransformHelpers
          .withSortedIdGroup[Balance]("addressId", "addressIdGroup", bucketSize)
      )
      .select("addressIdGroup", "addressId", "balance", "currency")
      .as[Balance]

    balance.union(balanceTokens)
  }

  def computeTransactionIds(
      transactions: Dataset[Transaction]
  ): Dataset[TransactionId] = {
    transactions
      .transform(onlySuccessfulTxs)
      .transform(removeUnknownRecipientTxs)
      .transform(txContractCreationAsToAddress)
      .as[Transaction]
      .map((row) =>
        TransactionId(
          row.txHash,
          computeMonotonicTxId(row.blockId, row.transactionIndex.toInt)
        )
      )

  }

  def computeAddressIdsByHash(
      traces: Dataset[Trace],
      transactions: Dataset[Transaction],
      tokenTransfers: Dataset[TokenTransfer],
      seed: Array[Byte] = Array(0, 0, 0, 0)
  ): Dataset[AddressIdLong] = {

    val txs = transactions
      .transform(onlySuccessfulTxs)
      .transform(txContractCreationAsToAddress)
      .transform(removeUnknownRecipientTxs)
    val trc = traces.transform(onlySuccessfulTrace)

    val fromAddress = trc
      .select(
        $"callerAddress".as("address")
      )

    val toAddress =
      trc.select(
        $"transfertoAddress".as("address")
      )

    val fromAddressTxs = txs
      .select(
        $"fromAddress".as("address")
      )

    val toAddressTxs = txs
      .select(
        $"toAddress".as("address")
      )

    val toAddressTT = tokenTransfers
      .select(
        $"to".as("address")
      )

    val fromAddressTT = tokenTransfers
      .select(
        $"from".as("address")
      )

    val all = fromAddress
      .union(toAddress)
      .union(fromAddressTxs)
      .union(toAddressTxs)
      .union(fromAddressTT)
      .union(toAddressTT)
      .filter($"address".isNotNull)

    val hashIds = all
      .select("address")
      .dropDuplicates()
      .withColumn("seed", lit(seed))
      .withColumn("h", xxhash64($"address", $"seed"))

    time("evaluate hashes for address ids: duplicate hashes") {
      val windowSpec = Window.partitionBy("address").orderBy("address")
      hashIds
        .withColumn("CountColumns", count($"h").over(windowSpec))
        .filter($"CountColumns" > 1)
        .drop("CountColumns")
        .show(100)
    }
    TransformHelpers.toDSEager[AddressIdLong](
      hashIds.withColumnRenamed("h", "addressId")
    )
  }

  def computeAddressIds(
      traces: Dataset[Trace],
      transactions: Dataset[Transaction],
      tokenTransfers: Dataset[TokenTransfer],
      maxTxsPerBlock: Long
  ): Dataset[AddressId] = {
    printStat("#traces", traces.count())
    printStat("#txs", transactions.count())
    printStat("#erc20 txs", tokenTransfers.count())

    val txs = transactions
      .transform(onlySuccessfulTxs)
      .transform(removeUnknownRecipientTxs)
    val trc = traces.transform(onlySuccessfulTrace)

    val fromAddress = trc
      .select(
        $"callerAddress".as("address"),
        $"blockId",
        $"traceIndex",
        lit(false).as("isLog")
      )
      .withColumn("isFromAddress", lit(true))

    val toAddress = trc
      .select(
        col("transfertoAddress").as("address"),
        col("blockId"),
        col("traceIndex"),
        lit(false).as("isLog")
      )
      .withColumn("isFromAddress", lit(false))

    val fromAddressTxs = txs
      .select(
        col("fromAddress").as("address"),
        col("blockId"),
        (col("transactionIndex") - lit(maxTxsPerBlock)).as("traceIndex"),
        lit(false).as("isLog")
      )
      .withColumn("isFromAddress", lit(true))

    val toAddressTxs = txs
      .select(
        col("toAddress").as("address"),
        col("blockId"),
        (col("transactionIndex") - lit(maxTxsPerBlock)).as("traceIndex"),
        lit(false).as("isLog")
      )
      .withColumn("isFromAddress", lit(false))

    val toAddressTT = tokenTransfers
      .select(
        col("to").as("address"),
        col("blockId"),
        col("logIndex").as("traceIndex"),
        lit(true).as("isLog")
      )
      .withColumn("isFromAddress", lit(false))

    val fromAddressTT = tokenTransfers
      .select(
        col("from").as("address"),
        col("blockId"),
        col("logIndex").as("traceIndex"),
        lit(true).as("isLog")
      )
      .withColumn("isFromAddress", lit(true))

    val orderWindow = Window
      .partitionBy("address")
      .orderBy("blockId", "traceIndex", "isFromAddress")

    val all = fromAddress
      .union(toAddress)
      .union(fromAddressTxs)
      .union(toAddressTxs)
      .union(fromAddressTT)
      .union(toAddressTT)
      .filter($"address".isNotNull)

    all
      .withColumn("rowNumber", row_number().over(orderWindow))
      .filter($"rowNumber" === 1)
      .sort("blockId", "isLog", "traceIndex", "isFromAddress")
      .select("address")
      .map(_.getAs[Array[Byte]]("address"))
      .rdd
      .zipWithIndex()
      .map { case ((a, id)) => AddressId(a, toIntSafe(id)) }
      .toDS()
  }

  def computeContracts(
      traces: Dataset[Trace],
      transactions: Dataset[Transaction],
      addressIds: Dataset[AddressId]
  ): Dataset[Contract] = {
    val traceDeployments = traces
      .transform(onlySuccessfulTrace)
      .filter(isCreationTrace)
      .select($"transfertoAddress".as("address"))

    val transactionDeployments = transactions
      .transform(onlySuccessfulTxs)
      .filter(isCreationTx)
      .select($"receiptContractAddress".as("address"))

    TransformHelpers.toDSEager[Contract](
      traceDeployments
        .union(transactionDeployments)
        .join(addressIds, Seq("address"))
        .select("addressId")
        .filter($"addressId".isNotNull)
        .distinct
    )
  }

  def computeEncodedTokenTransfers(
      tokenTransfers: Dataset[TokenTransfer],
      tokenConfigurations: Dataset[TokenConfiguration],
      transactionsIds: Dataset[TransactionId],
      addressIds: Dataset[AddressId],
      exchangeRates: Dataset[ExchangeRates]
  ): Dataset[EncodedTokenTransfer] = {
    ethTransform
      .computeEncodedTokenTransfers(
        tokenTransfers,
        tokenConfigurations,
        transactionsIds,
        addressIds,
        exchangeRates
      )
      .filter($"transactionId".isNotNull)
  }

  def computeEncodedTransactions(
      traces: Dataset[Trace],
      transactionsIds: Dataset[TransactionId],
      transactions: Dataset[Transaction],
      addressIds: Dataset[AddressId],
      exchangeRates: Dataset[ExchangeRates]
  ): Dataset[EncodedTransaction] = {

    def toFiatCurrency(valueColumn: String, fiatValueColumn: String)(
        df: DataFrame
    ) = {
      df.withColumn(
        fiatValueColumn,
        transform(
          col(fiatValueColumn),
          (x: Column) =>
            (col(valueColumn) * x / 1e6).cast(
              FloatType
            ) // should make it more generic and save native coin decimal somewhere centrally,
          // perhaps in the Tokens file? eth:18 trx:6
        )
      )
    }

    /*
    filtering null to addresses after we handled the
    contract creation txs, some txs eg. claim voting reward
    25ed08545d6384a7a455574086d7606157bcdd155d423a9ff345d0d7652110ea
    need special handling and additional infos to identify them
     */

    val txs = transactions
      .transform(onlySuccessfulTxs)
      .transform(txContractCreationAsToAddress)
      .transform(removeUnknownRecipientTxs)

    val trcs = traces
      .transform(onlySuccessfulTrxCallTraces)
      .filter($"txHash".isNotNull)

    val txsEncodedtemp = txs
      .drop(
        "txHashPrefix",
        "nonce",
        "blockHash",
        "transactionIndex",
        "gas",
        "gasPrice",
        "input",
        "blockTimestamp",
        "receiptGasUsed",
        "receiptStatus",
        "receiptContractAddress"
      )
      // .filter($"toAddress".isNotNull)
      .transform(joinTransactionIds(transactionsIds))
      .transform(joinAddressIds(addressIds, addressCol = "toAddress"))
      .withColumnRenamed("addressId", "dstAddressId")
      .transform(joinAddressIds(addressIds, addressCol = "fromAddress"))
      .withColumnRenamed("addressId", "srcAddressId")

    val txsEncoded = txsEncodedtemp.select(
      $"transactionId",
      $"blockId",
      lit(null).as("traceIndex"), // sort txs before traces
      $"srcAddressId",
      $"dstAddressId",
      $"value"
    )

    val tracesEncoded = trcs
      .drop(
        "note",
        "rejected"
      )
      .transform(joinTransactionIds(transactionsIds))
      .transform(joinAddressIds(addressIds, addressCol = "callerAddress"))
      .withColumnRenamed("addressId", "srcAddressId")
      .transform(joinAddressIds(addressIds, addressCol = "transfertoAddress"))
      .withColumnRenamed("addressId", "dstAddressId")
      .withColumnRenamed("callValue", "value")
      .select(
        $"transactionId",
        $"blockId",
        $"traceIndex",
        $"srcAddressId",
        $"dstAddressId",
        $"value"
      )

    TransformHelpers.toDSEager(
      txsEncoded
        .union(tracesEncoded)
        .filter(
          $"transactionId".isNotNull
        ) // there are apparently cases in the full dataset
        .join(broadcast(exchangeRates), Seq("blockId"), "left")
        .transform(toFiatCurrency("value", "fiatValues"))
    )
  }

  def computeBlockTransactions(
      blocks: Dataset[Block],
      encodedTransactions: Dataset[EncodedTransaction]
  ): Dataset[BlockTransactionRelational] = {
    TransformHelpers.toDSEager(
      encodedTransactions
        .select("blockId", "transactionId")
        .withColumnRenamed("transactionId", "txId")
        .filter($"txId".isNotNull)
        .dropDuplicates("blockId", "txId")
        .transform(
          TransformHelpers.withIdGroup("blockId", "blockIdGroup", bucketSize)
        )
    )
  }

  def computeAddressTransactions(
      encodedTransactions: Dataset[EncodedTransaction],
      encodedTokenTransfers: Dataset[EncodedTokenTransfer]
  ): Dataset[AddressTransaction] = {
    // ethTransform.computeAddressTransactions(
    //   encodedTransactions,
    //   encodedTokenTransfers,
    //   baseCurrencySymbol = "TRX"
    // )
    val baseCurrencySymbol = "TRX"
    val inputs = encodedTransactions
      .select(
        $"srcAddressId".as("addressId"),
        $"transactionId",
        $"traceIndex"
      )
      .withColumn("isOutgoing", lit(true))
      .withColumn("currency", lit(baseCurrencySymbol))
      .withColumn("logIndex", lit(null))

    val outputs = encodedTransactions
      .filter(col("dstAddressId").isNotNull)
      .select(
        $"dstAddressId".as("addressId"),
        $"transactionId",
        $"traceIndex"
      )
      .withColumn("isOutgoing", lit(false))
      .withColumn("currency", lit(baseCurrencySymbol))
      .withColumn("logIndex", lit(null))

    val inputsTokens = encodedTokenTransfers
      .withColumn("isOutgoing", lit(true))
      .withColumn("traceIndex", lit(null))
      .select(
        $"srcAddressId".as("addressId"),
        $"transactionId",
        $"traceIndex",
        $"isOutgoing",
        $"currency",
        $"logIndex"
      )

    val outputsTokens = encodedTokenTransfers
      .withColumn("isOutgoing", lit(false))
      .withColumn("traceIndex", lit(null))
      .select(
        $"dstAddressId".as("addressId"),
        $"transactionId",
        $"traceIndex",
        $"isOutgoing",
        $"currency",
        $"logIndex"
      )

    val uniontxs = inputs
      .union(inputsTokens)
      .union(outputs)
      .union(outputsTokens)

    // uniontxs.filter($"addressId".isNull || $"transactionId".isNull).show(100)

    val atxs = uniontxs
      .filter(
        $"addressId".isNotNull
      )
      .filter($"transactionId".isNotNull)
      .transform(
        TransformHelpers.withIdGroup("addressId", "addressIdGroup", bucketSize)
      )
      .transform(
        TransformHelpers.withSecondaryIdGroupSimple(
          "addressIdGroup",
          "addressIdSecondaryGroup",
          "transactionId",
          buckets = 128
        )
      )
      .transform(TransformHelpers.withTxReference)
      .drop("traceIndex", "logIndex")
      .sort(
        "addressId",
        "addressIdSecondaryGroup",
        "transactionId",
        "txReference"
      )

    TransformHelpers.toDSEager(atxs)
  }

  def computeAddresses(
      encodedTransactions: Dataset[EncodedTransaction],
      encodedTokenTransfers: Dataset[EncodedTokenTransfer],
      addressTransactions: Dataset[AddressTransaction],
      addressIds: Dataset[AddressId],
      contracts: Dataset[Contract]
  ): Dataset[Address] = {
    ethTransform.computeAddresses(
      encodedTransactions,
      encodedTokenTransfers,
      addressTransactions,
      addressIds,
      contracts
    )
  }

  def computeAddressRelations(
      encodedTransactions: Dataset[EncodedTransaction],
      encodedTokenTransfers: Dataset[EncodedTokenTransfer]
  ): Dataset[AddressRelation] = {
    ethTransform.computeAddressRelations(
      encodedTransactions,
      encodedTokenTransfers
    )
  }

  def summaryStatistics(
      lastBlockTimestamp: Int,
      noBlocks: Long,
      noTransactions: Long,
      noAddresses: Long,
      noAddressRelations: Long
  ) = {
    ethTransform.summaryStatistics(
      lastBlockTimestamp,
      noBlocks,
      noTransactions,
      noAddresses,
      noAddressRelations
    )
  }
}
