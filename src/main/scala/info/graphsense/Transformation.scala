package info.graphsense

import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{
  when,
  array,
  element_at,
  coalesce,
  col,
  collect_list,
  count,
  countDistinct,
  date_format,
  floor,
  from_unixtime,
  hex,
  lit,
  map_keys,
  map_values,
  max,
  min,
  row_number,
  size,
  struct,
  substring,
  sum,
  to_date,
  transform,
  typedLit,
  map_from_entries,
  collect_set
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
      fiatCurrencies: Seq[String]
  ) = {
    Seq(
      Configuration(
        keyspaceName,
        bucketSize,
        txPrefixLength,
        addressPrefixLength,
        fiatCurrencies
      )
    ).toDS()
  }

  def zeroValueIfNull(columnName: String, length: Int)(
      df: DataFrame
  ): DataFrame = {
    df.withColumn(
      columnName,
      coalesce(
        col(columnName),
        struct(
          lit(0).cast(DecimalType(38, 0)).as("value"),
          typedLit(Array.fill[Float](length)(0))
            .as("fiatValues")
        )
      )
    )
  }

  def aggregateValues(
      valueColumn: String,
      fiatValueColumn: String,
      length: Int,
      groupColumns: String*
  )(df: DataFrame): DataFrame = {
    df.groupBy(groupColumns.head, groupColumns.tail: _*)
      .agg(
        createAggCurrencyStruct(valueColumn, fiatValueColumn, length)
      )
  }

  def createAggCurrencyStruct(
      valueColumn: String,
      fiatValueColumn: String,
      length: Int
  ): Column = {
    struct(
      sum(col(valueColumn)).as(valueColumn),
      array(
        (0 until length)
          .map(i => sum(col(fiatValueColumn).getItem(i)).cast(FloatType)): _*
      ).as(fiatValueColumn)
    ).as(valueColumn)
  }

  def createAggCurrencyStructPerCurrency(
      valueColumn: String,
      fiatValueColumn: String,
      length: Int
  ): Column = {
    struct(
      col("currency"),
      createAggCurrencyStruct(valueColumn, fiatValueColumn, length)
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
      addressIds: Dataset[AddressId],
      token_transfers: Dataset[TokenTransfer],
      token_configurations: Dataset[TokenConfiguration]
  ): Dataset[Balance] = {

    val excludedCallTypes = Seq("delegatecall", "callcode", "staticcall")
    val callTypeFilter = (!col("callType").isin(excludedCallTypes: _*)) ||
      col("callType").isNull

    val debits = traces
      .filter(col("toAddress").isNotNull)
      .filter(col("status") === 1)
      .filter(callTypeFilter)
      .groupBy("toAddress")
      .agg(sum("value").as("debits"))
      .withColumnRenamed("toAddress", "address")
      .join(addressIds, Seq("address"), "left")
      .drop("address")

    val credits = traces
      .filter(col("fromAddress").isNotNull)
      .filter(col("status") === 1)
      .filter(callTypeFilter)
      .groupBy("fromAddress")
      .agg((-sum(col("value"))).as("credits"))
      .withColumnRenamed("fromAddress", "address")
      .join(addressIds, Seq("address"), "left")
      .drop("address")

    val txFeeDebits = transactions
      .join(blocks, Seq("blockId"), "inner")
      .withColumn("calculatedValue", col("receiptGasUsed") * col("gasPrice"))
      .groupBy("miner")
      .agg(sum("calculatedValue").as("txFeeDebits"))
      .withColumnRenamed("miner", "address")
      .join(addressIds, Seq("address"), "left")
      .drop("address")

    val txFeeCredits = transactions
      .withColumn("calculatedValue", -col("receiptGasUsed") * col("gasPrice"))
      .groupBy("fromAddress")
      .agg(sum("calculatedValue").as("txFeeCredits"))
      .withColumnRenamed("fromAddress", "address")
      .join(addressIds, Seq("address"), "left")
      .drop("address")

    val burntFees = blocks.na
      .fill(0, Seq("baseFeePerGas"))
      .withColumn(
        "value",
        -col("baseFeePerGas").cast(DecimalType(38, 0)) * col("gasUsed")
      )
      .groupBy("miner")
      .agg(sum("value").as("burntFees"))
      .withColumnRenamed("miner", "address")
      .join(addressIds, Seq("address"), "left")
      .drop("address")

    val balance = burntFees
      .join(debits, Seq("addressId"), "full")
      .join(credits, Seq("addressId"), "full")
      .join(txFeeDebits, Seq("addressId"), "full")
      .join(txFeeCredits, Seq("addressId"), "full")
      .na
      .fill(0)
      .withColumn(
        "balance",
        col("burntFees") +
          col("debits") + col("credits") +
          col("txFeeDebits") + col("txFeeCredits")
      )
      .withColumn("currency", lit("ETH"))
      .transform(withSortedIdGroup[Balance]("addressId", "addressIdGroup"))
      .select("addressIdGroup", "addressId", "balance", "currency")
      .as[Balance]

    val token_credits = token_transfers
      .groupBy("from", "token_address")
      .agg((-sum(col("value"))).as("credits"))
      .withColumnRenamed("from", "address")
      .join(addressIds, Seq("address"), "left")
      .drop("address")

    val token_debits = token_transfers
      .groupBy("to", "token_address")
      .agg((sum(col("value"))).as("debits"))
      .withColumnRenamed("to", "address")
      .join(addressIds, Seq("address"), "left")
      .drop("address")

    val balance_tokens_t = token_credits
      .join(token_debits, Seq("addressId", "token_address"), "full")
      .na
      .fill(0, Seq("credits", "debits"))
      .withColumn(
        "balance",
        col("debits") + col("credits")
      )
      .join(token_configurations, Seq("token_address"), "left")
      .withColumn("currency", col("currency_ticker"))

    val balance_tokens = balance_tokens_t
      .transform(withSortedIdGroup[Balance]("addressId", "addressIdGroup"))
      .select("addressIdGroup", "addressId", "balance", "currency")
      .as[Balance]

    balance.union(balance_tokens)
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
      traces: Dataset[Trace],
      token_transfers: Dataset[TokenTransfer]
  ): Dataset[AddressId] = {

    val fromAddress = traces
      .filter(col("status") === 1)
      .select(
        col("fromAddress").as("address"),
        col("blockId"),
        col("traceId")
      )
      .withColumn("isFromAddress", lit(true))
      .filter(col("address").isNotNull)

    val toAddress = traces
      .filter(col("status") === 1)
      .select(
        col("toAddress").as("address"),
        col("blockId"),
        col("traceId")
      )
      .withColumn("isFromAddress", lit(false))
      .filter(col("address").isNotNull)

    val toAddress_tt = token_transfers
      .select(
        col("to").as("address"),
        col("blockId"),
        col("logIndex").as("traceId")
      )
      .withColumn("isFromAddress", lit(false))

    val fromAddress_tt = token_transfers
      .select(
        col("from").as("address"),
        col("blockId"),
        col("logIndex").as("traceId")
      )
      .withColumn("isFromAddress", lit(true))

    val orderWindow = Window
      .partitionBy("address")
      .orderBy("blockId", "traceId", "isFromAddress")

    fromAddress
      .union(toAddress)
      .union(fromAddress_tt)
      .union(toAddress_tt)
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

  def computeContracts(
      traces: Dataset[Trace],
      addressIds: Dataset[AddressId]
  ): Dataset[Contract] = {
    traces
      .filter(col("status") === 1)
      .filter(col("traceId").startsWith("create"))
      .select(col("toAddress").as("address"))
      .join(addressIds, Seq("address"))
      .select($"addressId")
      .distinct
      .as[Contract]
  }

  def computeEncodedTokenTransfers(
      token_transfers: Dataset[TokenTransfer],
      token_configurations: Dataset[TokenConfiguration],
      transactionsIds: Dataset[TransactionId],
      addressIds: Dataset[AddressId],
      exchangeRates: Dataset[ExchangeRates]
  ): Dataset[EncodedTokenTransfer] = {
    def toFiatCurrency(valueColumn: String, fiatValueColumn: String)(
        df: DataFrame
    ) = {
      val df_with_stablecoin_factors = df.withColumn(
        fiatValueColumn,
        when(
          $"peg_currency" === "USD",
          array(
            lit(1),
            element_at(col(fiatValueColumn), 1) / element_at(
              col(fiatValueColumn),
              2
            )
          )
        ).otherwise(col(fiatValueColumn))
      )
      /*println(df_with_stablecoin_factors.show())*/
      df_with_stablecoin_factors.withColumn(
        fiatValueColumn,
        transform(
          col(fiatValueColumn),
          (x: Column) =>
            (col(valueColumn) * x / col("decimal_divisor")).cast(FloatType)
        )
      )
    }
    token_transfers
      .withColumnRenamed("txHash", "transaction")
      .join(
        transactionsIds,
        Seq("transaction"),
        "left"
      )
      .join(
        addressIds.select(
          col("address").as("from"),
          col("addressId").as("fromAddressId")
        ),
        Seq("from"),
        "left"
      )
      .join(
        addressIds.select(
          col("address").as("to"),
          col("addressId").as("toAddressId")
        ),
        Seq("to"),
        "left"
      )
      .drop(
        "blockHash",
        "txHashPrefix",
        "transactionIndex",
        "transaction",
        "from",
        "to"
      )
      .withColumnRenamed("fromAddressId", "srcAddressId")
      .withColumnRenamed("toAddressId", "dstAddressId")
      .join(exchangeRates, Seq("blockId"), "left")
      .join(
        token_configurations.select(
          "token_address",
          "currency_ticker",
          "peg_currency",
          "decimals",
          "decimal_divisor"
        ),
        Seq("token_address"),
        "left"
      )
      .transform(toFiatCurrency("value", "fiatValues"))
      .drop("decimals", "standard", "peg_currency", "decimal_divisor")
      .withColumnRenamed("currency_ticker", "currency")
      .as[EncodedTokenTransfer]
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
      df.withColumn(
        fiatValueColumn,
        transform(
          col(fiatValueColumn),
          (x: Column) => (col(valueColumn) * x / 1e18).cast(FloatType)
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
      encodedTransactions: Dataset[EncodedTransaction],
      encodedTokenTransfers: Dataset[EncodedTokenTransfer]
  ): Dataset[AddressTransaction] = {
    val inputs = encodedTransactions
      .select(
        col("srcAddressId").as("addressId"),
        col("transactionId")
        /*        col("blockId"),
        col("blockTimestamp")*/
      )
      .withColumn("isOutgoing", lit(true))
      .withColumn("currency", lit("ETH"))
      .withColumn("logIndex", lit(null))

    val outputs = encodedTransactions
      .filter(col("dstAddressId").isNotNull)
      .select(
        col("dstAddressId").as("addressId"),
        col("transactionId")
        /*        col("blockId"),
        col("blockTimestamp")*/
      )
      .withColumn("isOutgoing", lit(false))
      .withColumn("currency", lit("ETH"))
      .withColumn("logIndex", lit(null))

    val inputs_tokens = encodedTokenTransfers
      .withColumn("isOutgoing", lit(true))
      .select(
        col("srcAddressId").as("addressId"),
        col("transactionId"),
        col("isOutgoing"),
        col("currency"),
        col("logIndex")
        /*        col("blockId"),
        col("blockTimestamp")*/
      )

    val outputs_tokens = encodedTokenTransfers
      .withColumn("isOutgoing", lit(false))
      .select(
        col("dstAddressId").as("addressId"),
        col("transactionId"),
        col("isOutgoing"),
        col("currency"),
        col("logIndex")
        /*        col("blockId"),
        col("blockTimestamp")*/
      )
    inputs
      .union(inputs_tokens)
      .union(outputs)
      .union(outputs_tokens)
      .transform(withIdGroup("addressId", "addressIdGroup"))
      .transform(
        withSecondaryIdGroup(
          "addressIdGroup",
          "addressIdSecondaryGroup",
          "transactionId"
        )
      )
      .sort("addressId", "addressIdSecondaryGroup", "transactionId")
      .filter(
        col("addressId").isNotNull
      ) /*They cant be selected for anyways should only contain sender of coinbase*/
      .as[AddressTransaction]
  }

  def computeAddresses(
      encodedTransactions: Dataset[EncodedTransaction],
      encodedTokenTransfers: Dataset[EncodedTokenTransfer],
      addressTransactions: Dataset[AddressTransaction],
      addressIds: Dataset[AddressId],
      contracts: Dataset[Contract]
  ): Dataset[Address] = {

    val relations = encodedTransactions
      .select("dstAddressId", "srcAddressId", "transactionId")
      .union(
        encodedTokenTransfers
          .select("dstAddressId", "srcAddressId", "transactionId")
      )

    val outStats_eth = encodedTransactions
      .groupBy("srcAddressId")
      .agg(
        createAggCurrencyStruct("value", "fiatValues", noFiatCurrencies.get)
          .as("TotalSpent")
      )
    val outStats_relations = relations
      .groupBy("srcAddressId")
      .agg(
        count("transactionId").cast(IntegerType).as("noOutgoingTxs"),
        countDistinct("dstAddressId").cast(IntegerType).as("outDegree")
      )

    val outStats_token = encodedTokenTransfers
      .groupBy("srcAddressId", "currency")
      .agg(
        createAggCurrencyStructPerCurrency(
          "value",
          "fiatValues",
          noFiatCurrencies.get
        )
          .as("TokensSpent")
      )
      .groupBy("srcAddressId")
      .agg(
        map_from_entries(collect_set("TokensSpent")).as("totalTokensSpent")
      )

    val outStats = outStats_eth
      .join(outStats_relations, Seq("srcAddressId"), "full")
      .join(outStats_token, Seq("srcAddressId"), "full")
    /*
    println(outStats.sort("srcAddressId").show())*/

    val inStats_eth = encodedTransactions
      .groupBy("dstAddressId")
      .agg(
        createAggCurrencyStruct("value", "fiatValues", noFiatCurrencies.get)
          .as("TotalReceived")
      )

    val inStats_relations = relations
      .groupBy("dstAddressId")
      .agg(
        count("transactionId").cast(IntegerType).as("noIncomingTxs"),
        countDistinct("srcAddressId").cast(IntegerType).as("inDegree")
      )

    val inStats_token = encodedTokenTransfers
      .groupBy("dstAddressId", "currency")
      .agg(
        createAggCurrencyStructPerCurrency(
          "value",
          "fiatValues",
          noFiatCurrencies.get
        )
          .as("TokensReceived")
      )
      .groupBy("dstAddressId")
      .agg(
        map_from_entries(collect_set("TokensReceived"))
          .as("totalTokensReceived")
      )

    val inStats = inStats_eth
      .join(inStats_relations, Seq("dstAddressId"), "full")
      .join(inStats_token, Seq("dstAddressId"), "full")
    /*
    println(inStats.sort("dstAddressId").show())*/

    addressTransactions
      .select("addressId", "transactionId")
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
      .join(
        contracts.withColumn("isContract", lit(true)),
        Seq("addressId"),
        "left"
      )
      .na
      .fill(0, Seq("noIncomingTxs", "noOutgoingTxs", "inDegree", "outDegree"))
      .na
      .fill(false, Seq("isContract"))
      .transform(zeroValueIfNull("totalReceived", noFiatCurrencies.get))
      .transform(zeroValueIfNull("totalSpent", noFiatCurrencies.get))
      .join(addressIds, Seq("addressId"), "left")
      .transform(withIdGroup("addressId", "addressIdGroup"))
      .sort("addressId")
      .as[Address]
  }

  def computeAddressRelations(
      encodedTransactions: Dataset[EncodedTransaction],
      encodedTokenTransfers: Dataset[EncodedTokenTransfer]
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

    val aggValues_tokens = encodedTokenTransfers
      .groupBy("srcAddressId", "dstAddressId", "currency")
      .agg(
        createAggCurrencyStructPerCurrency(
          "value",
          "fiatValues",
          noFiatCurrencies.get
        ).as("tokenValues")
      )
      .groupBy("srcAddressId", "dstAddressId")
      .agg(
        map_from_entries(collect_set("tokenValues")).as("tokenValues")
      )

    val window = Window.partitionBy("srcAddressId", "dstAddressId")
    encodedTransactions
      .select("srcAddressId", "dstAddressId", "transactionId")
      // union token transfers to cover addresses that have only seen token transfers
      .union(
        encodedTokenTransfers.select(
          "srcAddressId",
          "dstAddressId",
          "transactionId"
        )
      )
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
      // join aggregated token values
      .join(
        aggValues_tokens,
        Seq("srcAddressId", "dstAddressId"),
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
      .transform(zeroValueIfNull("value", noFiatCurrencies.get))
      .as[AddressRelation]
  }

  def summaryStatistics(
      lastBlockTimestamp: Int,
      noBlocks: Long,
      noTransactions: Long,
      noAddresses: Long,
      noAddressRelations: Long
  ) = {
    Seq(
      SummaryStatistics(
        lastBlockTimestamp,
        lastBlockTimestamp,
        noBlocks,
        noBlocks,
        noTransactions,
        noAddresses,
        noAddressRelations
      )
    ).toDS()
  }
}
