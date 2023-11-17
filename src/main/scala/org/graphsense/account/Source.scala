package org.graphsense.account

import org.apache.spark.sql.Dataset
import org.graphsense.account.contract.tokens.TokenTransferHelper
import org.graphsense.account.models.{
  Block,
  Log,
  TokenConfiguration,
  TokenTransfer,
  Transaction
}
import org.graphsense.models.ExchangeRatesRaw
import org.graphsense.storage.CassandraStorage

trait AccountSource {

  def blocks(): Dataset[Block]
  def transactions(): Dataset[Transaction]
  def logs(): Dataset[Log]
  def exchangeRates(): Dataset[ExchangeRatesRaw]
  def tokenTransfers(): Dataset[TokenTransfer]
  def tokenConfigurations(): Dataset[TokenConfiguration]

}

trait TokenSet {

  def getSupportedTokens(): List[TokenConfiguration]

  def getTokenAddresses(): Seq[Array[Byte]] = {
    this.getSupportedTokens().map(x => x.tokenAddress)
  }
}

trait AccountSourceWithTokens extends AccountSource {

  protected def tokenTransferHelper: TokenTransferHelper

  def getTokenSet(): TokenSet

  def tokenTransfers(): Dataset[TokenTransfer] = {
    this.tokenTransferHelper
      .getTokenTransfers(this.logs(), this.getTokenSet().getTokenAddresses())
  }

  def tokenConfigurations(): Dataset[TokenConfiguration] = {
    this.tokenTransferHelper.getTokenConfigurations(
      this.getTokenSet().getSupportedTokens()
    )
  }

}

abstract class CassandraAccountSource(store: CassandraStorage, keyspace: String)
    extends AccountSourceWithTokens {

  private val spark = store.session()

  def tokenTransferHelper = new TokenTransferHelper(spark)

  import spark.implicits._

  def blocks(): Dataset[Block] = {
    store.load[Block](keyspace, "block")
  }

  def transactions(): Dataset[Transaction] = {
    store.load[Transaction](keyspace, "transactions")
  }

  def logs(): Dataset[Log] = {
    store
      .load[Log](
        keyspace,
        "log"
      )
  }

  def exchangeRates(): Dataset[ExchangeRatesRaw] = {
    store.load[ExchangeRatesRaw](keyspace, "exchange_rates")
  }

}
