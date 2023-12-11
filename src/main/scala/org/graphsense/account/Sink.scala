package org.graphsense.account

import org.apache.spark.sql.Dataset
import org.graphsense.account.models.{
  Address,
  AddressIdByAddressPrefix,
  AddressIncomingRelationSecondaryIds,
  AddressOutgoingRelationSecondaryIds,
  AddressRelation,
  AddressTransaction,
  AddressTransactionSecondaryIds,
  Balance,
  BlockTransaction,
  BlockTransactionRelational,
  Configuration,
  SummaryStatistics,
  TokenConfiguration,
  TransactionIdByTransactionIdGroup,
  TransactionIdByTransactionPrefix
}
import org.graphsense.models.ExchangeRates
import org.graphsense.storage.CassandraStorage

trait AccountSink {
  def saveTokenConfiguration(tokenConf: Dataset[TokenConfiguration]): Unit
  def saveConfiguration(conf: Dataset[Configuration]): Unit
  def saveExchangeRates(rates: Dataset[ExchangeRates]): Unit
  def saveTransactionIdsByGroup(
      ids: Dataset[TransactionIdByTransactionIdGroup]
  ): Unit
  def saveTransactionIdsByTxPrefix(
      ids: Dataset[TransactionIdByTransactionPrefix]
  ): Unit
  def saveAddressIdsByPrefix(ids: Dataset[AddressIdByAddressPrefix]): Unit
  def saveBalances(balances: Dataset[Balance]): Unit
  def saveBlockTransactions(blockTxs: Dataset[BlockTransaction]): Unit
  def saveBlockTransactionsRelational(
      blockTxs: Dataset[BlockTransactionRelational]
  ): Unit
  def saveAddressTransactions(addressTxs: Dataset[AddressTransaction]): Unit
  def saveAddressTransactionBySecondaryId(
      ids: Dataset[AddressTransactionSecondaryIds]
  ): Unit
  def saveAddresses(addresses: Dataset[Address]): Unit
  def saveAddressIncomingRelations(relations: Dataset[AddressRelation]): Unit
  def saveAddressOutgoingRelations(relations: Dataset[AddressRelation]): Unit
  def saveAddressOutgoingRelationsBySecondaryId(
      ids: Dataset[AddressOutgoingRelationSecondaryIds]
  ): Unit
  def saveAddressIncomingRelationsBySecondaryId(
      ids: Dataset[AddressIncomingRelationSecondaryIds]
  ): Unit
  def saveSummaryStatistics(statistic: Dataset[SummaryStatistics]): Unit
}

class CassandraAccountSink(store: CassandraStorage, keyspace: String)
    extends AccountSink {

  def saveConfiguration(conf: Dataset[Configuration]): Unit = {
    store.store(
      keyspace,
      "configuration",
      conf
    )
  }

  def saveTokenConfiguration(tokenConf: Dataset[TokenConfiguration]): Unit = {
    store.store(
      keyspace,
      "token_configuration",
      tokenConf
    )
  }

  def saveExchangeRates(rates: Dataset[ExchangeRates]): Unit = {
    store.store(keyspace, "exchange_rates", rates)
  }

  def saveTransactionIdsByGroup(
      ids: Dataset[TransactionIdByTransactionIdGroup]
  ): Unit = {
    store.store(
      keyspace,
      "transaction_ids_by_transaction_id_group",
      ids
    )
  }

  def saveTransactionIdsByTxPrefix(
      ids: Dataset[TransactionIdByTransactionPrefix]
  ): Unit = {
    store.store(
      keyspace,
      "transaction_ids_by_transaction_prefix",
      ids
    )
  }

  def saveAddressIdsByPrefix(ids: Dataset[AddressIdByAddressPrefix]): Unit = {
    store.store(
      keyspace,
      "address_ids_by_address_prefix",
      ids
    )
  }

  def saveBalances(balances: Dataset[Balance]): Unit = {
    store.store(keyspace, "balance", balances)
  }

  def saveBlockTransactions(blockTxs: Dataset[BlockTransaction]): Unit = {
    store.store(
      keyspace,
      "block_transactions",
      blockTxs
    )
  }

  def saveBlockTransactionsRelational(
      blockTxs: Dataset[BlockTransactionRelational]
  ): Unit = {
    store.store(
      keyspace,
      "block_transactions",
      blockTxs
    )
  }

  def saveAddressTransactions(
      addressTxs: Dataset[AddressTransaction]
  ): Unit = {
    store.store(
      keyspace,
      "address_transactions",
      addressTxs
    )
  }

  def saveAddressTransactionBySecondaryId(
      ids: Dataset[AddressTransactionSecondaryIds]
  ): Unit = {
    store.store(
      keyspace,
      "address_transactions_secondary_ids",
      ids
    )
  }

  def saveAddresses(addresses: Dataset[Address]): Unit = {
    store.store(keyspace, "address", addresses)
  }

  def saveAddressIncomingRelations(
      relations: Dataset[AddressRelation]
  ): Unit = {
    store.store(keyspace, "address_incoming_relations", relations)
  }

  def saveAddressOutgoingRelations(
      relations: Dataset[AddressRelation]
  ): Unit = {
    store.store(keyspace, "address_outgoing_relations", relations)
  }

  def saveAddressOutgoingRelationsBySecondaryId(
      ids: Dataset[AddressOutgoingRelationSecondaryIds]
  ): Unit = {
    store.store(
      keyspace,
      "address_outgoing_relations_secondary_ids",
      ids
    )
  }

  def saveAddressIncomingRelationsBySecondaryId(
      ids: Dataset[AddressIncomingRelationSecondaryIds]
  ): Unit = {
    store.store(
      keyspace,
      "address_incoming_relations_secondary_ids",
      ids
    )
  }

  def saveSummaryStatistics(statistic: Dataset[SummaryStatistics]): Unit = {
    store.store(
      keyspace,
      "summary_statistics",
      statistic
    )
  }

}
