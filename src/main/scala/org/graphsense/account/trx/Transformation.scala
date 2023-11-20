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
    throw new UnsupportedOperationException

  }

  def computeContracts(
      traces: Dataset[Trace],
      addressIds: Dataset[AddressId]
  ): Dataset[Contract] = {
    throw new UnsupportedOperationException
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
