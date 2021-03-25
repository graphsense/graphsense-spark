package at.ac.ait

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{
  col,
  collect_list,
  count,
  countDistinct,
  date_format,
  from_unixtime,
  hex,
  length,
  lit,
  lower,
  max,
  min,
  regexp_replace,
  row_number,
  struct,
  substring,
  sum,
  to_date,
  upper
}
import org.apache.spark.sql.types.{FloatType, IntegerType}

class Transformation(spark: SparkSession, bucketSize: Int) {

  import spark.implicits._

  def configuration(
      keyspaceName: String,
      bucketSize: Int
  ) = {
    Seq(
      Configuration(
        keyspaceName,
        bucketSize
      )
    ).toDS()
  }

  def computeExchangeRates(
      blocks: Dataset[Block],
      exchangeRates: Dataset[ExchangeRatesRaw]
  ): Dataset[ExchangeRates] = {
    val blocksDate = blocks
      .withColumn(
        "date",
        date_format(
          to_date(from_unixtime(col("timestamp"), "yyyy-MM-dd")),
          "yyyy-MM-dd"
        )
      )
      .select("number", "date")

    val lastDateExchangeRates =
      exchangeRates.select(max(col("date"))).first.getString(0)
    val lastDateBlocks = blocksDate.select(max(col("date"))).first.getString(0)
    if (lastDateExchangeRates < lastDateBlocks)
      println(
        "WARNING: exchange rates not available for all blocks, filling missing values with 0"
      )

    blocksDate
      .join(exchangeRates, Seq("date"), "left")
      .na
      .fill(0)
      .drop("date")
      .sort("number")
      .withColumnRenamed("number", "height")
      .as[ExchangeRates]
  }

  def computeTransactionIds(
      transactions: Dataset[Transaction]
  ): Dataset[TransactionId] = {
    transactions
      .select("blockNumber", "transactionIndex", "hash")
      .sort("blockNumber", "transactionIndex")
      .select("hash")
      .map(_.getAs[Array[Byte]]("hash"))
      .rdd
      .zipWithIndex()
      .map { case ((tx, id)) => TransactionId(tx, id.toInt) }
      .toDS()
  }

  def computeAddressIds(
      transactions: Dataset[Transaction]
  ): Dataset[AddressId] = {
    val fromAddress = transactions
      .select(
        col("fromAddress").as("address"),
        col("blockNumber"),
        col("transactionIndex")
      )
      .withColumn("isFromAddress", lit(true))
    val toAddress = transactions
      .select(
        col("toAddress").as("address"),
        col("blockNumber"),
        col("transactionIndex")
      )
      .withColumn("isFromAddress", lit(false))
      .filter(col("address").isNotNull)

    val orderWindow = Window
      .partitionBy("address")
      .orderBy("blockNumber", "transactionIndex", "isFromAddress")

    fromAddress
      .union(toAddress)
      .withColumn("rowNumber", row_number().over(orderWindow))
      .filter(col("rowNumber") === 1)
      .sort("blockNumber", "transactionIndex", "isFromAddress")
      .select("address")
      .map(_ getAs [Array[Byte]] ("address"))
      .rdd
      .zipWithIndex()
      .map { case ((a, id)) => AddressId(a, id.toInt) }
      .toDS()
  }

  def computeEncodedTransactions(
      transactions: Dataset[Transaction],
      transactionsIds: Dataset[TransactionId],
      addressIds: Dataset[AddressId],
      exchangeRates: Dataset[ExchangeRates]
  ): Dataset[EncodedTransaction] = {
    def toFiatCurrency(valueColumn: String)(df: DataFrame) = {
      df.withColumn(
        valueColumn,
        struct(
          col(valueColumn),
          (col(valueColumn) / 1e18 * col("usd")).cast(FloatType).as("usd"),
          (col(valueColumn) / 1e18 * col("eur")).cast(FloatType).as("eur")
        )
      )
    }
    transactions
      .withColumnRenamed("hash", "transaction")
      .join(
        transactionsIds,
        Seq("transaction"),
        "left"
      )
      .join(
        addressIds.select(
          col("address").as("fromAddress"),
          col("addressId").as("fromAddressId")
        ),
        Seq("fromAddress"),
        "left"
      )
      .join(
        addressIds.select(
          col("address").as("toAddress"),
          col("addressId").as("toAddressId")
        ),
        Seq("toAddress"),
        "left"
      )
      .drop(
        "blockHash",
        "hashPrefix",
        "transaction",
        "toAddress",
        "fromAddress"
      )
      .withColumnRenamed("blockNumber", "height")
      .withColumnRenamed("fromAddressId", "srcAddressId")
      .withColumnRenamed("toAddressId", "dstAddressId")
      .join(exchangeRates, Seq("height"), "left")
      .transform(toFiatCurrency("value"))
      .drop("usd", "eur")
      .as[EncodedTransaction]
  }

//  def computeAddressByIdGroups(
//      addressIds: Dataset[AddressId]
//  ): Dataset[AddressByIdGroup] = {
//    addressIds
//      .select("addressId", "address")
//      .transform(t.idGroup("addressId", "addressIdGroup"))
//      .as[AddressByIdGroup]
//  }

  def computeBlockTransactions(
      blocks: Dataset[Block],
      encodedTransactions: Dataset[EncodedTransaction]
  ): Dataset[BlockTransaction] = {
    encodedTransactions
      .groupBy("height")
      .agg(collect_list("transactionId").as("txs"))
      .join(
        blocks.select(col("number").as("height")),
        Seq("height"),
        "right"
      )
      .sort("height")
      .as[BlockTransaction]
  }

  def computeAddressTransactions(
      encodedTransactions: Dataset[EncodedTransaction]
  ): Dataset[AddressTransaction] = {
    val inputs = encodedTransactions
      .select(
        col("srcAddressId").as("addressId"),
        col("transactionId"),
        col("value")("value").as("value"),
        col("height"),
        col("blockTimestamp")
      )
      .withColumn("value", -col("value"))
    val outputs = encodedTransactions
      .filter(col("dstAddressId").isNotNull)
      .select(
        col("dstAddressId").as("addressId"),
        col("transactionId"),
        col("value")("value").as("value"),
        col("height"),
        col("blockTimestamp")
      )

    inputs
      .union(outputs)
      .sort("addressId", "transactionId")
      .as[AddressTransaction]
  }

  def computeAddressTags(
      tags: Dataset[TagRaw],
      addressIds: Dataset[AddressId],
      currency: String
  ): Dataset[AddressTag] = {
    tags
      .filter(col("currency") === currency)
      .drop(col("currency"))
      .withColumn(
        "address",
        // make uppercase and remove first two charcter from string (0x)
        upper(col("address")).substr(lit(3), length(col("address")) - 2)
      )
      .join(
        addressIds
          .select(hex(col("address")).as("address"), col("addressId")),
        Seq("address"),
        "inner"
      )
      .as[AddressTag]
  }

  def computeAddresses(
      encodedTransactions: Dataset[EncodedTransaction],
      addressTransactions: Dataset[AddressTransaction]
  ): Dataset[Address] = {
    val outStats = encodedTransactions
      .groupBy("srcAddressId")
      .agg(
        count("transactionId").cast(IntegerType).as("noOutgoingTxs"),
        countDistinct("dstAddressId").cast(IntegerType).as("outDegree"),
        struct(
          sum(col("value.value")).as("value"),
          sum(col("value.usd")).cast(FloatType).as("usd"),
          sum(col("value.eur")).cast(FloatType).as("eur")
        ).as("totalSpent")
      )
    val inStats = encodedTransactions
      .groupBy("dstAddressId")
      .agg(
        count("transactionId").cast(IntegerType).as("noIncomingTxs"),
        countDistinct("srcAddressId").cast(IntegerType).as("inDegree"),
        struct(
          sum(col("value.value")).as("value"),
          sum(col("value.usd")).cast(FloatType).as("usd"),
          sum(col("value.eur")).cast(FloatType).as("eur")
        ).as("totalReceived")
      )
    val txTimestamp = addressTransactions
      .select(
        col("transactionId"),
        struct("height", "transactionId", "blockTimestamp")
      )
      .dropDuplicates()

    addressTransactions
      .groupBy("addressId")
      .agg(
        min(col("transactionId")).as("firstTxId"),
        max(col("transactionId")).as("lastTxId")
      )
      .join(
        txTimestamp.toDF("firstTxId", "firstTx"),
        Seq("firstTxId"),
        "left"
      )
      .as("firstTx")
      .join(
        txTimestamp.toDF("lastTxId", "lastTx"),
        Seq("lastTxId"),
        "left"
      )
      .drop("firstTxId", "lastTxId")
      .join(
        inStats.withColumnRenamed("dstAddressId", "addressId"),
        Seq("addressId"),
        "left"
      )
      .join(
        outStats.withColumnRenamed("srcAddressId", "addressId"),
        Seq("addressId"),
        "left"
      )
      .na
      .fill(0, Seq("noIncomingTxs", "noOutgoingTxs", "inDegree", "outDegree"))
      .as[Address]
  }

  def computeAddressRelations(
      encodedTransactions: Dataset[EncodedTransaction],
      addresses: Dataset[Address]
  ): Dataset[AddressRelation] = {
    encodedTransactions
      .filter(col("dstAddressId").isNotNull)
      .filter(col("value").isNotNull)
      .groupBy("srcAddressId", "dstAddressId")
      .agg(
        count(col("transactionId")) cast IntegerType as "noTransactions",
        struct(
          sum(col("value.value")).as("value"),
          sum(col("value.usd")).cast(FloatType).as("usd"),
          sum(col("value.eur")).cast(FloatType).as("eur")
        ).as("value")
      )
      .join(
        addresses
          .select(
            col("addressId").as("srcAddressId"),
            struct("totalReceived", "totalSpent").as("srcProperties")
          ),
        Seq("srcAddressId"),
        "left"
      )
      .join(
        addresses
          .select(
            col("addressId").as("dstAddressId"),
            struct("totalReceived", "totalSpent").as("dstProperties")
          ),
        Seq("dstAddressId"),
        "left"
      )
      .as[AddressRelation]
  }

  def computeTagsByLabel(
      tags: Dataset[TagRaw],
      addressTags: Dataset[AddressTag],
      currency: String,
      prefixLength: Int = 3
  ): Dataset[Tag] = {
    // check if addresses where used in transactions
    tags
      .filter(col("currency") === currency)
      .join(
        addressTags
          .select(col("address"))
          .withColumn("activeAddress", lit(true)),
        Seq("address"),
        "left"
      )
      .na
      .fill(false, Seq("activeAddress"))
      // normalize labels
      .withColumn(
        "labelNorm",
        lower(regexp_replace(col("label"), "[\\W_]+", ""))
      )
      .withColumn(
        "labelNormPrefix",
        substring(col("labelNorm"), 0, prefixLength)
      )
      .as[Tag]
  }

  /*
  def summaryStatistics(
      lastBlockTimestamp: Int,
      noBlocks: Int,
      noTransactions: Long,
      noAddresses: Long,
      noAddressRelations: Long,
      noTags: Long,
  ) = {
    Seq(
      SummaryStatistics(
        lastBlockTimestamp,
        noBlocks,
        noTransactions,
        noAddresses,
        noAddressRelations,
        noTags
      )
    ).toDS()
  }
 */
}
