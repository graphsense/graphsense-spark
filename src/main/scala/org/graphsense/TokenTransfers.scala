package org.graphsense

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{col, lit, udf}
import org.graphsense.Conversion._
import org.graphsense.contract.tokens.Erc20
import scala.math.pow
import scala.util.{Failure, Success, Try}

class TokenTransfers(spark: SparkSession) {

  import spark.implicits._

  val supportedTokens = List(
    TokenConfiguration(
      "USDT",
      hexStrToBytes("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
      "erc20",
      6,
      pow(10, 6).longValue(),
      Some("USD")
    ),
    TokenConfiguration(
      "USDC",
      hexStrToBytes("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
      "erc20",
      6,
      pow(10, 6).longValue(),
      Some("USD")
    ),
    TokenConfiguration(
      "WETH",
      hexStrToBytes("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"),
      "erc20",
      18,
      pow(10, 18).longValue(),
      Some("ETH")
    )
  )

  val tokenAddresses = supportedTokens.map(x => x.tokenAddress)

  def getTokenConfigurations(): Dataset[TokenConfiguration] = {
    spark
      .createDataFrame(
        supportedTokens
      )
      .as[TokenConfiguration]
  }

  def getTokenTransfers(
      logs: Dataset[Log],
      forTokens: Seq[Array[Byte]]
  ): Dataset[TokenTransfer] = {
    logs
      .filter(col("topic0") === lit(Erc20.transferTopicHash))
      .filter(col("address").isin(forTokens: _*))
      .map(x => Erc20.decodeTransfer(x))
      .filter((x: Try[TokenTransfer]) => x.isSuccess)
      .map(x => x.get)
  }

  def getNonDecodableTransferLogs(logs: Dataset[Log]): Dataset[Log] = {
    logs
      .filter(col("topic0") === lit(Erc20.transferTopicHash))
      .filter(col("address").isin(tokenAddresses: _*))
      .filter(x =>
        Erc20.decodeTransfer(x) match {
          case Success(_) => false
          case Failure(_) => true
        }
      )
  }

  def humanReadableTokenTransfers(
      transfers: Dataset[TokenTransfer]
  ): DataFrame = {
    val htoStr = udf((x: Array[Byte]) => bytesToHexStr(x))
    val transfersStr = transfers
      .withColumn("blockId", transfers("blockId"))
      .withColumn("transactionIndex", transfers("transactionIndex"))
      .withColumn("logIndex", transfers("logIndex"))
      .withColumn("txhash", htoStr(transfers("txhash")))
      .withColumn("tokenAddress", htoStr(transfers("tokenAddress")))
      .withColumn("from", htoStr(transfers("from")))
      .withColumn("to", htoStr(transfers("to")))
      .withColumn("value", transfers("value"))
    transfersStr.toDF
  }
}
