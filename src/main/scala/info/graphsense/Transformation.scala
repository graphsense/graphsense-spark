package info.graphsense

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
  unix_timestamp,
  upper
}
import org.apache.spark.sql.types.{DecimalType, FloatType, IntegerType}

class Transformation(spark: SparkSession, bucketSize: Int) {

  import spark.implicits._

  private var noFiatCurrencies: Option[Int] = None

  def configuration(
      keyspaceName: String,
      bucketSize: Int,
      txPrefixLength: Int,
      addressPrefixLength: Int,
      labelPrefixLength: Int,
      fiatCurrencies: Seq[String]
  ) = {
    Seq(
      Configuration(
        keyspaceName,
        bucketSize,
        txPrefixLength,
        addressPrefixLength,
        labelPrefixLength,
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
      idColumn: String,
      idGroupColumn: String
  )(df: DataFrame): Dataset[T] = {
    df.transform(withIdGroup(idColumn, idGroupColumn))
      .as[T]
      .sort(idGroupColumn)
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
      prefixColumn: String,
      length: Int = 4
  )(df: DataFrame): Dataset[T] = {
    df.transform(withPrefix(hashColumn, prefixColumn, length))
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
      .sort(primaryPartitionColumn)
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
      .select("blockId", "date")

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
      .sort("blockId")
      .as[ExchangeRates]
  }

  def computeBalances(
      blocks: Dataset[Block],
      transactions: Dataset[Transaction],
      traces: Dataset[Trace],
      addressIds: Dataset[AddressId]
  ): Dataset[Balance] = {

    val excludedCallTypes = Seq("delegatecall", "callcode", "staticcall")
    val callTypeFilter = (!col("callType").isin(excludedCallTypes: _*)) ||
      col("callType").isNull

    val debits = traces
      .filter(col("toAddress").isNotNull)
      .filter(col("status") === 1)
      .filter(callTypeFilter)
      .select(col("toAddress").as("address"), col("value"))

    val credits = traces
      .filter(col("fromAddress").isNotNull)
      .filter(col("status") === 1)
      .filter(callTypeFilter)
      .withColumn("invertedValue", -col("value"))
      .select(
        col("fromAddress").as("address"),
        col("invertedValue").as("value")
      )

    val txFeeDebits = transactions
      .join(blocks, Seq("blockId"), "inner")
      .withColumn("calculatedValue", col("receiptGasUsed") * col("gasPrice"))
      .groupBy("miner")
      .agg(sum("calculatedValue").as("value"))
      .select(col("miner").as("address"), col("value"))

    val txFeeCredits = transactions
      .withColumn("calculatedValue", -col("receiptGasUsed") * col("gasPrice"))
      .select(
        col("fromAddress").as("address"),
        col("calculatedValue").as("value")
      )

    val burntFees = blocks.na
      .fill(0, Seq("baseFeePerGas"))
      .withColumn(
        "value",
        -col("baseFeePerGas").cast(DecimalType(38, 0)) * col("gasUsed")
      )
      .select(col("miner").as("address"), col("value"))
      .groupBy("address")
      .agg(sum("value").as("value"))

    val rows =
      Seq(credits, debits, txFeeCredits, txFeeDebits, burntFees)
        .reduce(_ union _)
        .join(addressIds, Seq("address"), "left")
        .drop("address")

    rows
      .groupBy("addressId")
      .agg(sum("value").as("balance"))
      .transform(withSortedIdGroup[Balance]("addressId", "addressIdGroup"))
      .as[Balance]
  }

  def computeTransactionIds(
      transactions: Dataset[Transaction]
  ): Dataset[TransactionId] = {
    transactions
      .select("blockId", "transactionIndex", "txhash")
      .sort("blockId", "transactionIndex")
      .select("txHash")
      .map(_.getAs[Array[Byte]]("txHash"))
      .rdd
      .zipWithIndex()
      .map { case ((tx, id)) => TransactionId(tx, id.toInt) }
      .toDS()
  }

  def computeAddressIds(
      traces: Dataset[Trace]
  ): Dataset[AddressId] = {

    val fromAddress = traces
      .select(
        col("fromAddress").as("address"),
        col("blockId"),
        col("traceId")
      )
      .withColumn("isFromAddress", lit(true))
      .filter(col("address").isNotNull)

    val toAddress = traces
      .select(
        col("toAddress").as("address"),
        col("blockId"),
        col("traceId")
      )
      .withColumn("isFromAddress", lit(false))
      .filter(col("address").isNotNull)

    val orderWindow = Window
      .partitionBy("address")
      .orderBy("blockId", "traceId", "isFromAddress")

    fromAddress
      .union(toAddress)
      .withColumn("rowNumber", row_number().over(orderWindow))
      .filter(col("rowNumber") === 1)
      .sort("blockId", "traceId", "isFromAddress")
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
      .withColumnRenamed("txHash", "transaction")
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
        "txHashPrefix",
        "transaction",
        "toAddress",
        "fromAddress",
        "receiptGasUsed"
      )
      .withColumnRenamed("fromAddressId", "srcAddressId")
      .withColumnRenamed("toAddressId", "dstAddressId")
      .join(exchangeRates, Seq("blockId"), "left")
      .transform(toFiatCurrency("value", "fiatValues"))
      .as[EncodedTransaction]
  }

  def computeBlockTransactions(
      blocks: Dataset[Block],
      encodedTransactions: Dataset[EncodedTransaction]
  ): Dataset[BlockTransaction] = {
    encodedTransactions
      .groupBy("blockId")
      .agg(collect_list("transactionId").as("txs"))
      .join(
        blocks.select(col("blockId")),
        Seq("blockId"),
        "right"
      )
      .transform(withIdGroup("blockId", "blockIdGroup"))
      .sort("blockId")
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
        col("blockId"),
        col("blockTimestamp")
      )
      .withColumn("value", -col("value"))
      .withColumn("isOutgoing", lit(true))
    val outputs = encodedTransactions
      .filter(col("dstAddressId").isNotNull)
      .select(
        col("dstAddressId").as("addressId"),
        col("transactionId"),
        col("value"),
        col("blockId"),
        col("blockTimestamp")
      )
      .withColumn("isOutgoing", lit(false))

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
      tags: Dataset[AddressTagRaw],
      addressIds: Dataset[AddressId],
      currency: String
  ): Dataset[AddressTag] = {
    tags
      .filter(col("currency") === currency)
      .drop(col("currency"))
      .withColumn(
        "address",
        // make uppercase and remove first two characters from string (0x)
        upper(col("address")).substr(lit(3), length(col("address")) - 2)
      )
      .join(
        addressIds
          .select(hex(col("address")).as("address"), col("addressId")),
        Seq("address"),
        "inner"
      )
      .drop("address")
      .withColumn(
        "lastmod",
        unix_timestamp(col("lastmod"), "yyyy-dd-MM").cast(IntegerType)
      )
      .transform(withSortedIdGroup[AddressTag]("addressId", "addressIdGroup"))
  }

  def computeAddresses(
      encodedTransactions: Dataset[EncodedTransaction],
      addressTransactions: Dataset[AddressTransaction],
      addressIds: Dataset[AddressId]
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

    addressTransactions
      .groupBy("addressId")
      .agg(
        min(col("transactionId")).as("firstTxId"),
        max(col("transactionId")).as("lastTxId")
      )
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
      .join(addressIds, Seq("addressId"), "left")
      .transform(withIdGroup("addressId", "addressIdGroup"))
      .sort("addressId")
      .as[Address]
  }

  def computeAddressRelations(
      encodedTransactions: Dataset[EncodedTransaction],
      addressTags: Dataset[AddressTag]
  ): Dataset[AddressRelation] = {

    val aggValues = encodedTransactions.toDF.transform(
      aggregateValues(
        "value",
        "fiatValues",
        noFiatCurrencies.get,
        "srcAddressId",
        "dstAddressId"
      )
    )

    val addressLabels = addressTags
      .select("addressId")
      .distinct
      .withColumn("hasLabels", lit(true))

    val window = Window.partitionBy("srcAddressId", "dstAddressId")
    encodedTransactions
      .filter(col("dstAddressId").isNotNull)
      .withColumn(
        "noTransactions",
        count(col("transactionId")).over(window).cast(IntegerType)
      )
      .groupBy("srcAddressId", "dstAddressId")
      // aggregate to number of transactions and list of transaction ids
      .agg(
        min("noTransactions").as("noTransactions")
      )
      // join aggregated currency values
      .join(
        aggValues,
        Seq("srcAddressId", "dstAddressId"),
        "left"
      )
      // join boolean column to indicate presence of src labels
      .join(
        addressLabels.select(
          col("addressId").as("srcAddressId"),
          col("hasLabels").as("hasSrcLabels")
        ),
        Seq("srcAddressId"),
        "left"
      )
      // join boolean column to indicate presence of dst labels
      .join(
        addressLabels.select(
          col("addressId").as("dstAddressId"),
          col("hasLabels").as("hasDstLabels")
        ),
        Seq("dstAddressId"),
        "left"
      )
      // add partitioning columns for outgoing addresses
      .transform(withIdGroup("srcAddressId", "srcAddressIdGroup"))
      .transform(
        withSecondaryIdGroup(
          "srcAddressIdGroup",
          "srcAddressIdSecondaryGroup",
          "srcAddressId"
        )
      )
      // add partitioning columns for incoming addresses
      .transform(withIdGroup("dstAddressId", "dstAddressIdGroup"))
      .transform(
        withSecondaryIdGroup(
          "dstAddressIdGroup",
          "dstAddressIdSecondaryGroup",
          "dstAddressId"
        )
      )
      .na
      .fill(false, Seq("hasSrcLabels", "hasDstLabels"))
      .as[AddressRelation]
  }

  def computeTagsByLabel(
      tags: Dataset[AddressTagRaw],
      addressTags: Dataset[AddressTag],
      addressIds: Dataset[AddressId],
      currency: String,
      prefixLength: Int
  ): Dataset[Tag] = {
    // check if addresses where used in transactions
    tags
      .filter(col("currency") === currency)
      .withColumn(
        "address",
        // make uppercase and remove first two characters from string (0x)
        upper(col("address")).substr(lit(3), length(col("address")) - 2)
      )
      .join(
        addressIds
          .select(hex(col("address")).as("address"), col("addressId")),
        Seq("address"),
        "left"
      )
      .join(
        addressTags
          .select(col("addressId"))
          .withColumn("activeAddress", lit(true)),
        Seq("addressId"),
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
      .withColumn(
        "lastmod",
        unix_timestamp(col("lastmod"), "yyyy-dd-MM").cast(IntegerType)
      )
      .drop("addressId")
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
