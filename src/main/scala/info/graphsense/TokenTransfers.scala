package info.graphsense
import info.graphsense.Conversion._
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.functions.{col, lit, udf}
import info.graphsense.contract.tokens.Erc20
import org.apache.spark.sql.SparkSession
import scala.util.Try
import scala.util.Success
import scala.util.Failure

class TokenTransfers(spark: SparkSession) {

  import spark.implicits._

  val supported_tokens = List(
    TokenConfiguration(
      "USDT",
      hexstr_to_bytes("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
      "erc20",
      6
    ),
    TokenConfiguration(
      "USDC",
      hexstr_to_bytes("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
      "erc20",
      6
    )
  )

  val token_addresses = supported_tokens.map(x => x.token_address)

  def get_token_configurations(): Dataset[TokenConfiguration] = {
    spark
      .createDataFrame(
        supported_tokens
      )
      .as[TokenConfiguration]
  }

  def get_token_transfers(logs: Dataset[Log]): Dataset[TokenTransfer] = {
    logs
      .filter(col("topic0") === lit(Erc20.transfer_topic_hash))
      .filter(col("address").isin(token_addresses: _*))
      .map(x => Erc20.decode_transfer(x))
      .filter((x: Try[TokenTransfer]) => x.isSuccess)
      .map(x => x.get)
  }

  def get_non_decodable_transfer_logs(logs: Dataset[Log]): Dataset[Log] = {
    logs
      .filter(col("topic0") === lit(Erc20.transfer_topic_hash))
      .filter(col("address").isin(token_addresses: _*))
      .filter(x =>
        Erc20.decode_transfer(x) match {
          case Success(_) => false
          case Failure(_) => true
        }
      )
  }

  def human_readable_token_transfers(
      transfers: Dataset[TokenTransfer]
  ): DataFrame = {
    val htostr = udf((x: Array[Byte]) => bytes_to_hexstr(x))
    val transfers_str = transfers
      .withColumn("blockId", transfers("blockId"))
      .withColumn("transactionIndex", transfers("transactionIndex"))
      .withColumn("logIndex", transfers("logIndex"))
      .withColumn("txhash", htostr(transfers("txhash")))
      .withColumn("token_address", htostr(transfers("token_address")))
      .withColumn("from", htostr(transfers("from")))
      .withColumn("to", htostr(transfers("to")))
      .withColumn("value", transfers("value"))
    transfers_str.toDF
  }

}
