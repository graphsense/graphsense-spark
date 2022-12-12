package info.graphsense
import info.graphsense.Conversion._
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.functions.{col, lit, udf}
import info.graphsense.contract.tokens.Erc20
import org.apache.spark.sql.SparkSession

class TokenTransfers(spark: SparkSession) {

  import spark.implicits._

  val supported_tokens = List(
    ("USDT", hexstr_to_bytes("0xdAC17F958D2ee523a2206206994597C13D831ec7"))
  )

  val token_addresses = supported_tokens.map(x => x._2)

  def get_token_transfers(logs: Dataset[Log]): Dataset[TokenTransfer] = {
    logs
      .filter(col("topic0") === lit(Erc20.transfer_topic_hash))
      .filter(col("address").isin(token_addresses: _*))
      .map(x => Erc20.decode_transfer(x).get)
  }

  def human_readable_token_transfers(
      transfers: Dataset[TokenTransfer]
  ): DataFrame = {
    val htostr = udf((x: Array[Byte]) => bytes_to_hexstr(x))
    val transfers_str = transfers
      .withColumn("txhash", htostr(transfers("txhash")))
      .withColumn("token_address", htostr(transfers("token_address")))
      .withColumn("from", htostr(transfers("from")))
      .withColumn("to", htostr(transfers("to")))
      .withColumn("value", transfers("value"))
    transfers_str.toDF
  }

}
