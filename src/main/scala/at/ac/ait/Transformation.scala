package at.ac.ait

import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{
  array,
  coalesce,
  col,
  collect_list,
  count,
  countDistinct,
  date_format,
  floor,
  from_unixtime,
  hex,
  length,
  lit,
  lower,
  map_keys,
  map_values,
  max,
  min,
  regexp_replace,
  row_number,
  size,
  struct,
  substring,
  sum,
  to_date,
  typedLit,
  upper
}
import org.apache.spark.sql.types.{DecimalType, FloatType, IntegerType}

class Transformation(spark: SparkSession, bucketSize: Int) {

  import spark.implicits._

  private var noFiatCurrencies: Option[Int] = None

  def configuration(
      keyspaceName: String,
      bucketSize: Int,
      fiatCurrencies: Seq[String]
  ) = {
    Seq(
      Configuration(
        keyspaceName,
        bucketSize,
        fiatCurrencies
      )
    ).toDS()
  }

  def aggregateValues(
      valueColumn: String,
      fiatValueColumn: String,
      length: Int,
      groupColumns: String*
  )(df: DataFrame): DataFrame = {
    df.groupBy(groupColumns.head, groupColumns.tail: _*)
      .agg(
        struct(
          sum(col(valueColumn)).as(valueColumn),
          array(
            (0 until length)
              .map(i => sum(col(fiatValueColumn).getItem(i)).cast(FloatType)): _*
          ).as(fiatValueColumn)
        ).as(valueColumn)
      )
  }

  def getFiatCurrencies(
      exchangeRatesRaw: Dataset[ExchangeRatesRaw]
  ): Seq[String] = {
    val currencies =
      exchangeRatesRaw.select(map_keys(col("fiatValues"))).distinct
    if (currencies.count() > 1L)
      throw new Exception("Non-unique map keys in raw exchange rates table")
    currencies.rdd.map(r => r(0).asInstanceOf[Seq[String]]).collect()(0)
  }

  def withIdGroup[T](
      idColumn: String,
      idGroupColumn: String,
      size: Int = bucketSize
  )(ds: Dataset[T]): DataFrame = {
    ds.withColumn(idGroupColumn, floor(col(idColumn) / size).cast("int"))
  }

  def withSortedIdGroup[T: Encoder](
      hashColumn: String,
      prefixColumn: String
  )(df: DataFrame): Dataset[T] = {
    df.transform(withIdGroup(hashColumn, prefixColumn))
      .as[T]
      .sort(prefixColumn)
  }

  def withPrefix[T](
      hashColumn: String,
      hashPrefixColumn: String,
      length: Int = 4
  )(ds: Dataset[T]): DataFrame = {
    ds.withColumn(hashPrefixColumn, substring(hex(col(hashColumn)), 0, length))
  }

  def withSortedPrefix[T: Encoder](
      hashColumn: String,
      prefixColumn: String
  )(df: DataFrame): Dataset[T] = {
    df.transform(withPrefix(hashColumn, prefixColumn))
      .as[T]
      .sort(prefixColumn)
  }

  def withSecondaryIdGroup[T](
      idColumn: String,
      secondaryIdColumn: String,
      windowOrderColumn: String,
      skewedPartitionFactor: Float = 2.5f
  )(ds: Dataset[T]): DataFrame = {
    val partitionSize =
      ds.select(col(idColumn)).groupBy(idColumn).count().persist()
    val noPartitions = partitionSize.count()
    val approxMedian = partitionSize
      .sort(col("count").asc)
      .select(col("count"))
      .rdd
      .zipWithIndex
      .filter(_._2 == noPartitions / 2)
      .map(_._1)
      .first()
      .getLong(0)
    val window = Window.partitionBy(idColumn).orderBy(windowOrderColumn)
    ds.withColumn(
      secondaryIdColumn,
      floor(
        row_number().over(window) / (approxMedian * skewedPartitionFactor)
      ).cast(IntegerType)
    )
  }

  def computeSecondaryPartitionIdLookup[T: Encoder](
      df: DataFrame,
      primaryPartitionColumn: String,
      secondaryPartitionColumn: String
  ): Dataset[T] = {
    df.groupBy(primaryPartitionColumn)
      .agg(max(secondaryPartitionColumn).as("maxSecondaryId"))
      // to save storage space, store only records with multiple secondary IDs
      .filter(col("maxSecondaryId") > 0)
      .as[T]
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

    noFiatCurrencies = Some(
      exchangeRates.select(size(col("fiatValues"))).distinct.first.getInt(0)
    )

    blocksDate
      .join(exchangeRates, Seq("date"), "left")
      // replace null values in column fiatValues
      .withColumn("fiatValues", map_values(col("fiatValues")))
      .withColumn(
        "fiatValues",
        coalesce(
          col("fiatValues"),
          typedLit(Array.fill[Float](noFiatCurrencies.get)(0))
        )
      )
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
      .map(_.getAs[Array[Byte]]("address"))
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
    def toFiatCurrency(valueColumn: String, fiatValueColumn: String)(
        df: DataFrame
    ) = {
      // see `transform_values` in Spark 3
      df.withColumn(
        fiatValueColumn,
        array(
          (0 until noFiatCurrencies.get)
            .map(
              i =>
                (col(valueColumn) / 1e18 * col(fiatValueColumn).getItem(i))
                  .cast(FloatType)
            ): _*
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
      .transform(toFiatCurrency("value", "fiatValues"))
      .as[EncodedTransaction]
  }

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
      .transform(withIdGroup("height", "heightGroup"))
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
        col("value"),
        col("height"),
        col("blockTimestamp")
      )
      .withColumn("value", -col("value"))
    val outputs = encodedTransactions
      .filter(col("dstAddressId").isNotNull)
      .select(
        col("dstAddressId").as("addressId"),
        col("transactionId"),
        col("value"),
        col("height"),
        col("blockTimestamp")
      )

    inputs
      .union(outputs)
      .transform(withIdGroup("addressId", "addressIdGroup"))
      .transform(
        withSecondaryIdGroup(
          "addressIdGroup",
          "addressIdSecondaryGroup",
          "transactionId"
        )
      )
      .sort("addressId", "addressIdSecondaryGroup", "transactionId")
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
    def zeroValueIfNull(columnName: String)(df: DataFrame): DataFrame = {
      df.withColumn(
        columnName,
        coalesce(
          col(columnName),
          struct(
            lit(0).cast(DecimalType(38, 0)).as("value"),
            typedLit(Array.fill[Float](noFiatCurrencies.get)(0))
              .as("fiatValues")
          )
        )
      )
    }
    val outStats = encodedTransactions
      .groupBy("srcAddressId")
      .agg(
        count("transactionId").cast(IntegerType).as("noOutgoingTxs"),
        countDistinct("dstAddressId").cast(IntegerType).as("outDegree")
      )
      .join(
        encodedTransactions.toDF.transform(
          aggregateValues(
            "value",
            "fiatValues",
            noFiatCurrencies.get,
            "srcAddressId"
          )
        ),
        Seq("srcAddressId"),
        "left"
      )
      .withColumnRenamed("value", "TotalSpent")
    val inStats = encodedTransactions
      .groupBy("dstAddressId")
      .agg(
        count("transactionId").cast(IntegerType).as("noIncomingTxs"),
        countDistinct("srcAddressId").cast(IntegerType).as("inDegree")
      )
      .join(
        encodedTransactions.toDF
          .transform(
            aggregateValues(
              "value",
              "fiatValues",
              noFiatCurrencies.get,
              "dstAddressId"
            )
          ),
        Seq("dstAddressId"),
        "left"
      )
      .withColumnRenamed("value", "TotalReceived")
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
      .transform(zeroValueIfNull("totalReceived"))
      .transform(zeroValueIfNull("totalSpent"))
      .transform(withIdGroup("addressId", "addressIdGroup"))
      .sort("addressId")
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
        count(col("transactionId")) cast IntegerType as "noTransactions"
      )
      .join(
        encodedTransactions.toDF.transform(
          aggregateValues(
            "value",
            "fiatValues",
            noFiatCurrencies.get,
            "srcAddressId",
            "dstAddressId"
          )
        ),
        Seq("srcAddressId", "dstAddressId"),
        "left"
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
      .transform(withIdGroup("srcAddressId", "srcAddressIdGroup"))
      .transform(
        withSecondaryIdGroup(
          "srcAddressIdGroup",
          "srcAddressIdSecondaryGroup",
          "srcAddressId"
        )
      )
      .transform(withIdGroup("dstAddressId", "dstAddressIdGroup"))
      .transform(
        withSecondaryIdGroup(
          "dstAddressIdGroup",
          "dstAddressIdSecondaryGroup",
          "dstAddressId"
        )
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

  def summaryStatistics(
      lastBlockTimestamp: Int,
      noBlocks: Long,
      noTransactions: Long,
      noAddresses: Long,
      noAddressRelations: Long,
      noTags: Long
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
}
