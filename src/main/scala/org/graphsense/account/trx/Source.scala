package org.graphsense.account.trx

import com.datastax.spark.connector.ColumnName
import org.apache.spark.sql.Dataset
import org.graphsense.account.{AccountSource, CassandraAccountSource, TokenSet}
import org.graphsense.account.trx.models.{Trace, Trc10}
import org.graphsense.account.trx.tokens.TrxTokenSet
import org.graphsense.storage.CassandraStorage

trait TrxSource extends AccountSource {

  def traces(): Dataset[Trace]

  def trc10Tokens(): Dataset[Trc10]

}

class CassandraTrxSource(store: CassandraStorage, keyspace: String)
    extends CassandraAccountSource(store, keyspace)
    with TrxSource {

  override def getTokenSet(): TokenSet = {
    TrxTokenSet
  }

  def trc10Tokens(): Dataset[Trc10] = {
    val spark = store.session()
    import spark.implicits._
    store.load[Trc10](
      keyspace,
      "trc10",
      Array(
        "id",
        "ownerAddress",
        "name",
        "abbr",
        "description",
        "totalSupply",
        "url",
        "precision"
      ).map(
        ColumnName(_)
      ): _*
    )
  }

  def traces(): Dataset[Trace] = {
    val spark = store.session()
    import spark.implicits._
    store.load[Trace](
      keyspace,
      "trace",
      Array(
        "block_id_group",
        "block_id",
        "internal_index",
        "trace_index",
        "caller_address",
        "transferto_address",
        "call_info_index",
        "call_token_id",
        "call_value",
        "note",
        "rejected",
        "tx_hash"
      ).map(
        ColumnName(_)
      ): _*
    )
  }
}
