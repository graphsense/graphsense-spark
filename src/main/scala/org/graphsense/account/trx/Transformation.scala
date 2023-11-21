package org.graphsense.account.trx

import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{
  array,
  broadcast,
  coalesce,
  col,
  collect_set,
  count,
  countDistinct,
  date_format,
  element_at,
  from_unixtime,
  lit,
  map_from_entries,
  map_values,
  max,
  min,
  row_number,
  size,
  sort_array,
  sum,
  to_date,
  transform,
  typedLit,
  when
}
import org.apache.spark.sql.types.{DecimalType, FloatType, IntegerType}
import org.graphsense.TransformHelpers
import org.graphsense.account.trx.models._
import org.graphsense.account.models._
import org.graphsense.models.{ExchangeRates, ExchangeRatesRaw}
import org.graphsense.account.eth.EthTransformation

class TrxTransformation(spark: SparkSession, bucketSize: Int) {

  import spark.implicits._

  val ethTransform = new EthTransformation(spark, bucketSize)

  private var noFiatCurrencies: Option[Int] = None

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
    throw new UnsupportedOperationException
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
      .filter(col("callValue") > 0) // not sure; nothing transferred, so do we need it?
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
      .filter(col("callValue") > 0) // not sure; nothing transferred, so do we need it?
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
    throw new UnsupportedOperationException
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
