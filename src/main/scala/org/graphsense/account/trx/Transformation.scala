package org.graphsense.account.trx

import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{
  broadcast,
  col,
  lit,
  row_number,
  sum,
  transform
}
import org.apache.spark.sql.types.{DecimalType, FloatType}
import org.graphsense.TransformHelpers
import org.graphsense.account.trx.models._
import org.graphsense.account.models._
import org.graphsense.models.{ExchangeRates, ExchangeRatesRaw}
import org.graphsense.account.eth.EthTransformation

class TrxTransformation(spark: SparkSession, bucketSize: Int) {

  import spark.implicits._

  val ethTransform = new EthTransformation(spark, bucketSize)

  None

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

  def computeBalances(
      blocks: Dataset[Block],
      transactions: Dataset[Transaction],
      traces: Dataset[Trace],
      addressIds: Dataset[AddressId],
      tokenTransfers: Dataset[TokenTransfer],
      tokenConfigurations: Dataset[TokenConfiguration]
  ): Dataset[Balance] = {
    val callFilter = col("callTokenId").isNull && col("rejected") == false

    val traceDebits = traces
      .filter(callFilter)
      .groupBy("transfertoAddress")
      .agg(sum("callValue").as("traceDebits"))
      .withColumnRenamed("transfertoAddress", "address")
      .join(addressIds, Seq("address"), "left")
      .drop("address")

    val traceCredits = traces
      .filter(callFilter)
      .groupBy("callerAddress")
      .agg((-sum(col("callValue"))).as("traceCredits"))
      .withColumnRenamed("callerAddress", "address")
      .join(addressIds, Seq("address"), "left")
      .drop("address")

    val txDebits = transactions
      .filter(col("receiptStatus") === 1)
      .groupBy("toAddress")
      .agg(sum("value").as("txDebits"))
      .withColumnRenamed("toAddress", "address")
      .join(addressIds, Seq("address"), "left")
      .drop("address")

    val txCredits = transactions
      .filter(col("receiptStatus") === 1)
      .groupBy("fromAddress")
      .agg((-sum("value")).as("txCredits"))
      .withColumnRenamed("fromAddress", "address")
      .join(addressIds, Seq("address"), "left")
      .drop("address")

    val txFeeDebits = transactions
      .join(blocks, Seq("blockId"), "inner")
      .withColumn("calculatedValue", col("receiptGasUsed") * col("gasPrice"))
      .groupBy("miner")
      .agg(sum("calculatedValue").as("txFeeDebits"))
      .withColumnRenamed("miner", "address")
      .join(addressIds, Seq("address"), "left")
      .drop("address")

    val txFeeCredits = transactions
      .withColumn("calculatedValue", -col("receiptGasUsed") * col("gasPrice"))
      .groupBy("fromAddress")
      .agg(sum("calculatedValue").as("txFeeCredits"))
      .withColumnRenamed("fromAddress", "address")
      .join(addressIds, Seq("address"), "left")
      .drop("address")

    val burntFees = blocks.na
      .fill(0, Seq("baseFeePerGas"))
      .withColumn(
        "value",
        -col("baseFeePerGas").cast(DecimalType(38, 0)) * col("gasUsed")
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
        col("burntFees") +
          col("traceDebits") + col("traceCredits") +
          col("txDebits") + col("txCredits") +
          col("txFeeDebits") + col("txFeeCredits")
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
      .agg((-sum(col("value"))).as("credits"))
      .withColumnRenamed("from", "address")
      .join(addressIds, Seq("address"), "left")
      .drop("address")

    val tokenDebits = tokenTransfers
      .groupBy("to", "tokenAddress")
      .agg((sum(col("value"))).as("debits"))
      .withColumnRenamed("to", "address")
      .join(addressIds, Seq("address"), "left")
      .drop("address")

    val balanceTokensTmp = tokenCredits
      .join(tokenDebits, Seq("addressId", "tokenAddress"), "full")
      .na
      .fill(0, Seq("credits", "debits"))
      .withColumn(
        "balance",
        col("debits") + col("credits")
      )
      .join(tokenConfigurations, Seq("tokenAddress"), "left")
      .withColumn("currency", col("currencyTicker"))

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
    ethTransform.computeTransactionIds(transactions)
  }

  def computeAddressIds(
      traces: Dataset[Trace],
      tokenTransfers: Dataset[TokenTransfer]
  ): Dataset[AddressId] = {
    // tron traces:
    //  block_id_group | block_id | trace_index | call_info_index | call_token_id | call_value |
    //  caller_address | internal_index | note | rejected | transferto_address  | tx_hash
    // ethereum traces:
    //  vs block_id_group | block_id | trace_index | call_type | error | from_address | gas
    //  | gas_used | input | output | reward_type | status | subtraces | to_address
    //  | trace_address | trace_id  | trace_type | transaction_index | tx_hash | value

    val fromAddress = traces
      .filter(
        col(
          "callValue"
        ) > 0 // Do we ever need zero-call-value-traces? - could pull this upstream
      ) // not sure; nothing transferred, so do we need it?
      .filter(col("rejected") === false)
      .select(
        col("callerAddress").as("address"),
        col("blockId"),
        col("traceIndex"),
        lit(false).as("isLog")
      )
      .withColumn("isFromAddress", lit(true))
      .filter(col("address").isNotNull)

    val toAddress = traces
      .filter(
        col(
          "callValue"
        ) > 0 // Do we ever need zero-call-value-traces? - could pull this upstream
      ) // not sure; nothing transferred, so do we need it?
      .filter(col("rejected") === false)
      .select(
        col("transfertoAddress").as("address"),
        col("blockId"),
        col("traceIndex"),
        lit(false).as("isLog")
      )
      .withColumn("isFromAddress", lit(false))
      .filter(col("address").isNotNull)

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

    fromAddress
      .union(toAddress)
      .union(fromAddressTT)
      .union(toAddressTT)
      .withColumn("rowNumber", row_number().over(orderWindow))
      .filter(col("rowNumber") === 1)
      .sort("blockId", "isLog", "traceIndex", "isFromAddress")
      .select("address")
      .map(_.getAs[Array[Byte]]("address"))
      .rdd
      .zipWithIndex()
      .map { case ((a, id)) => AddressId(a, id.toInt) }
      .toDS()
  }

  def computeContracts(
      traces: Dataset[Trace],
      addressIds: Dataset[AddressId]
  ): Dataset[Contract] = {
    traces
      .filter(col("rejected") === false)
      .filter(col("note") === "create")
      .select(col("transfertoAddress").as("address"))
      .join(addressIds, Seq("address"))
      .select("addressId")
      .distinct
      .as[Contract]
  }

  def computeEncodedTokenTransfers(
      tokenTransfers: Dataset[TokenTransfer],
      tokenConfigurations: Dataset[TokenConfiguration],
      transactionsIds: Dataset[TransactionId],
      addressIds: Dataset[AddressId],
      exchangeRates: Dataset[ExchangeRates]
  ): Dataset[EncodedTokenTransfer] = {
    ethTransform.computeEncodedTokenTransfers(
      tokenTransfers,
      tokenConfigurations,
      transactionsIds,
      addressIds,
      exchangeRates
    )
  }

  def computeEncodedTransactions(
      traces: Dataset[Trace],
      transactionsIds: Dataset[TransactionId],
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

    traces
      .filter(col("rejected") === false)
      .filter(col("callTokenId").isNull) // could be trc10 values otherwise
      // .filter(
      //  col("callValue") > 0 // Do we ever need zero-call-value-traces? - could pull this upstream
      // ) // not sure; nothing transferred, so do we need it?
      .withColumnRenamed("txHash", "transaction")
      .join(
        transactionsIds,
        Seq("transaction"),
        "left"
      )
      .join(
        addressIds
          .withColumnRenamed("address", "callerAddress")
          .withColumnRenamed("addressId", "fromAddressId"),
        Seq("callerAddress"),
        "left"
      )
      .join(
        addressIds
          .withColumnRenamed("address", "transfertoAddress")
          .withColumnRenamed("addressId", "toAddressId"),
        Seq("transfertoAddress"),
        "left"
      )
      .drop(
        "blockIdGroup",
        "status",
        "callType",
        "transfertoAddress",
        "callerAddress",
        "receiptGasUsed",
        "transaction",
        "traceId"
      )
      .withColumnRenamed("fromAddressId", "srcAddressId")
      .withColumnRenamed("toAddressId", "dstAddressId")
      .join(broadcast(exchangeRates), Seq("blockId"), "left")
      .withColumnRenamed("callValue", "value")
      .transform(toFiatCurrency("value", "fiatValues"))
      .as[EncodedTransaction]
  }

  def computeBlockTransactions(
      blocks: Dataset[Block],
      encodedTransactions: Dataset[EncodedTransaction]
  ): Dataset[BlockTransaction] = {
    ethTransform.computeBlockTransactions(blocks, encodedTransactions)
  }

  def computeAddressTransactions(
      encodedTransactions: Dataset[EncodedTransaction],
      encodedTokenTransfers: Dataset[EncodedTokenTransfer]
  ): Dataset[AddressTransaction] = {
    ethTransform.computeAddressTransactions(
      encodedTransactions,
      encodedTokenTransfers
    )
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
