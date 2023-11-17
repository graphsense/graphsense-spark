package org.graphsense.account.trx

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
  TransactionIdByTransactionIdGroup,
  TransactionIdByTransactionPrefix
}
import org.graphsense.TransformHelpers

class TronJob(
    spark: SparkSession,
    source: TrxSource,
    sink: AccountSink,
    config: AccountConfig
) extends Job {
  import spark.implicits._

  // private val transformation = new EthTransformation(spark, config.bucketSize())

  def run(from: Option[Integer], to: Option[Integer]): Unit = {
    val exchangeRatesRaw = source.exchangeRates()
    val blocks = source.blocks()
    val transactions = source.transactions()
    val traces = source.traces()
    val tokenConfigurations = source.tokenConfigurations().persist()
    val tokenTransfers = source.tokenTransfers()

  }

}
