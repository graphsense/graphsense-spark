package org.graphsense.account.eth

import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{
  array,
  broadcast,
  coalesce,
  col,
  collect_set,
  count,
  countDistinct,
  date_format,
  element_at,
  from_unixtime,
  lit,
  map_from_entries,
  map_values,
  max,
  min,
  row_number,
  size,
  sort_array,
  sum,
  to_date,
  transform,
  typedLit,
  when
}
import org.apache.spark.sql.types.{DecimalType, FloatType, IntegerType}
import org.graphsense.TransformHelpers
import org.graphsense.account.eth.models._
import org.graphsense.account.models._
import org.graphsense.models.{ExchangeRates, ExchangeRatesRaw}

class EthTransformation(spark: SparkSession, bucketSize: Int) {

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

    val maxDateExchangeRates =
      exchangeRates.select(max(col("date"))).first.getString(0)
    val maxDateBlocks = blocksDate.select(max(col("date"))).first.getString(0)
    if (maxDateExchangeRates < maxDateBlocks) {
      val noBlocksRemove =
        blocksDate.filter(col("date") > maxDateExchangeRates).count()
      println(
        s"WARNING: exchange rates not available for all blocks, removing ${noBlocksRemove} blocks"
      )
    }

    noFiatCurrencies = Some(
      exchangeRates.select(size(col("fiatValues"))).distinct.first.getInt(0)
    )

    blocksDate
      .join(exchangeRates, Seq("date"), "left")
      // remove blocks with missing exchange rate values at the end
      .filter(col("date") <= maxDateExchangeRates)
      .withColumn("fiatValues", map_values(col("fiatValues")))
      // fill remaining missing values in column fiatValue with zeros
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
      tokenTransfers: Dataset[TokenTransfer],
      tokenConfigurations: Dataset[TokenConfiguration]
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
      .transform(
        TransformHelpers
          .withSortedIdGroup[Balance]("addressId", "addressIdGroup", bucketSize)
      )
      .select("addressIdGroup", "addressId", "balance", "currency")
      .as[Balance]

    val tokenCredits = tokenTransfers
      .groupBy("from", "tokenAddress")
      .agg((-sum(col("value"))).as("credits"))
      .withColumnRenamed("from", "address")
      .join(addressIds, Seq("address"), "left")
      .drop("address")

    val tokenDebits = tokenTransfers
      .groupBy("to", "tokenAddress")
      .agg((sum(col("value"))).as("debits"))
      .withColumnRenamed("to", "address")
      .join(addressIds, Seq("address"), "left")
      .drop("address")

    val balanceTokensTmp = tokenCredits
      .join(tokenDebits, Seq("addressId", "tokenAddress"), "full")
      .na
      .fill(0, Seq("credits", "debits"))
      .withColumn(
        "balance",
        col("debits") + col("credits")
      )
      .join(tokenConfigurations, Seq("tokenAddress"), "left")
      .withColumn("currency", col("currencyTicker"))

    val balanceTokens = balanceTokensTmp
      .transform(
        TransformHelpers
          .withSortedIdGroup[Balance]("addressId", "addressIdGroup", bucketSize)
      )
      .select("addressIdGroup", "addressId", "balance", "currency")
      .as[Balance]

    balance.union(balanceTokens)
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
      tokenTransfers: Dataset[TokenTransfer]
  ): Dataset[AddressId] = {

    val fromAddress = traces
      .filter(col("status") === 1)
      .select(
        col("fromAddress").as("address"),
        col("blockId"),
        col("traceIndex"),
        lit(false).as("isLog")
      )
      .withColumn("isFromAddress", lit(true))
      .filter(col("address").isNotNull)

    val toAddress = traces
      .filter(col("status") === 1)
      .select(
        col("toAddress").as("address"),
        col("blockId"),
        col("traceIndex"),
        lit(false).as("isLog")
      )
      .withColumn("isFromAddress", lit(false))
      .filter(col("address").isNotNull)

    val toAddressTT = tokenTransfers
      .select(
        col("to").as("address"),
        col("blockId"),
        col("logIndex").as("traceIndex"),
        lit(true).as("isLog")
      )
      .withColumn("isFromAddress", lit(false))

    val fromAddressTT = tokenTransfers
      .select(
        col("from").as("address"),
        col("blockId"),
        col("logIndex").as("traceIndex"),
        lit(true).as("isLog")
      )
      .withColumn("isFromAddress", lit(true))

    val orderWindow = Window
      .partitionBy("address")
      .orderBy("blockId", "traceIndex", "isFromAddress")

    fromAddress
      .union(toAddress)
      .union(fromAddressTT)
      .union(toAddressTT)
      .withColumn("rowNumber", row_number().over(orderWindow))
      .filter(col("rowNumber") === 1)
      .sort("blockId", "isLog", "traceIndex", "isFromAddress")
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
      .select("addressId")
      .distinct
      .as[Contract]
  }

  def computeEncodedTokenTransfers(
      tokenTransfers: Dataset[TokenTransfer],
      tokenConfigurations: Dataset[TokenConfiguration],
      transactionsIds: Dataset[TransactionId],
      addressIds: Dataset[AddressId],
      exchangeRates: Dataset[ExchangeRates]
  ): Dataset[EncodedTokenTransfer] = {
    def toFiatCurrency(valueColumn: String, fiatValueColumn: String)(
        df: DataFrame
    ) = {
      val dfWithStablecoinFactors = df.withColumn(
        fiatValueColumn,
        when(
          col("pegCurrency") === "USD",
          array(
            element_at(col(fiatValueColumn), 1) / element_at(
              col(fiatValueColumn),
              2
            ),
            lit(1)
          )
        ).otherwise(col(fiatValueColumn))
      )
      dfWithStablecoinFactors.withColumn(
        fiatValueColumn,
        transform(
          col(fiatValueColumn),
          (x: Column) =>
            (col(valueColumn) * x / col("decimalDivisor")).cast(FloatType)
        )
      )
    }
    tokenTransfers
      .withColumnRenamed("txHash", "transaction")
      .join(
        transactionsIds,
        Seq("transaction"),
        "left"
      )
      .join(
        addressIds
          .withColumnRenamed("address", "from")
          .withColumnRenamed("addressId", "fromAddressId"),
        Seq("from"),
        "left"
      )
      .join(
        addressIds
          .withColumnRenamed("address", "to")
          .withColumnRenamed("addressId", "toAddressId"),
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
        tokenConfigurations.select(
          "tokenAddress",
          "currencyTicker",
          "pegCurrency",
          "decimals",
          "decimalDivisor"
        ),
        Seq("tokenAddress"),
        "left"
      )
      .transform(toFiatCurrency("value", "fiatValues"))
      .drop("decimals", "standard", "pegCurrency", "decimalDivisor")
      .withColumnRenamed("currencyTicker", "currency")
      .as[EncodedTokenTransfer]
  }

  def computeEncodedTransactions(
      traces: Dataset[Trace],
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
    traces
      .filter(col("status") === 1)
      .withColumnRenamed("txHash", "transaction")
      .join(
        transactionsIds,
        Seq("transaction"),
        "left"
      )
      .join(
        addressIds
          .withColumnRenamed("address", "fromAddress")
          .withColumnRenamed("addressId", "fromAddressId"),
        Seq("fromAddress"),
        "left"
      )
      .join(
        addressIds
          .withColumnRenamed("address", "toAddress")
          .withColumnRenamed("addressId", "toAddressId"),
        Seq("toAddress"),
        "left"
      )
      .drop(
        "blockIdGroup",
        "status",
        "callType",
        "toAddress",
        "fromAddress",
        "receiptGasUsed",
        "transaction",
        "traceId"
      )
      .withColumnRenamed("fromAddressId", "srcAddressId")
      .withColumnRenamed("toAddressId", "dstAddressId")
      .join(broadcast(exchangeRates), Seq("blockId"), "left")
      .transform(toFiatCurrency("value", "fiatValues"))
      .as[EncodedTransaction]
  }

  def computeBlockTransactions(
      blocks: Dataset[Block],
      encodedTransactions: Dataset[EncodedTransaction]
  ): Dataset[BlockTransaction] = {
    encodedTransactions
      .groupBy("blockId")
      .agg(collect_set("transactionId").as("txs"))
      .withColumn("txs", sort_array(col("txs")))
      .join(
        blocks.select(col("blockId")),
        Seq("blockId"),
        "right"
      )
      .transform(
        TransformHelpers.withIdGroup("blockId", "blockIdGroup", bucketSize)
      )
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
        col("transactionId"),
        col("traceIndex")
      )
      .withColumn("isOutgoing", lit(true))
      .withColumn("currency", lit("ETH"))
      .withColumn("logIndex", lit(null))

    val outputs = encodedTransactions
      .filter(col("dstAddressId").isNotNull)
      .select(
        col("dstAddressId").as("addressId"),
        col("transactionId"),
        col("traceIndex")
      )
      .withColumn("isOutgoing", lit(false))
      .withColumn("currency", lit("ETH"))
      .withColumn("logIndex", lit(null))

    val inputsTokens = encodedTokenTransfers
      .withColumn("isOutgoing", lit(true))
      .withColumn("traceIndex", lit(null))
      .select(
        col("srcAddressId").as("addressId"),
        col("transactionId"),
        col("traceIndex"),
        col("isOutgoing"),
        col("currency"),
        col("logIndex")
      )

    val outputsTokens = encodedTokenTransfers
      .withColumn("isOutgoing", lit(false))
      .withColumn("traceIndex", lit(null))
      .select(
        col("dstAddressId").as("addressId"),
        col("transactionId"),
        col("traceIndex"),
        col("isOutgoing"),
        col("currency"),
        col("logIndex")
      )

    val atxs = inputs
      .union(inputsTokens)
      .union(outputs)
      .union(outputsTokens)
      .transform(
        TransformHelpers.withIdGroup("addressId", "addressIdGroup", bucketSize)
      )
      .transform(
        TransformHelpers.withSecondaryIdGroup(
          "addressIdGroup",
          "addressIdSecondaryGroup",
          "transactionId"
        )
      )
      .transform(TransformHelpers.withTxReference)
      .drop("traceIndex", "logIndex")
      .sort(
        "addressId",
        "addressIdSecondaryGroup",
        "transactionId",
        "txReference"
      )
      .filter(
        col("addressId").isNotNull
      ) /*They cant be selected for anyways should only contain sender of coinbase*/

    /*    val txWithoutTxIds = atxs.filter(col("transactionId").isNull)
    val nr_of_txs_without_ids = txWithoutTxIds.count()
    if (nr_of_txs_without_ids > 0) {
      println(
        "Found address_transactions without txid: " + nr_of_txs_without_ids
      )
      println(txWithoutTxIds.show(100, false))
    }*/

    atxs.filter(col("transactionId").isNotNull).as[AddressTransaction]
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

    val outStatsEth = encodedTransactions
      .groupBy("srcAddressId")
      .agg(
        TransformHelpers
          .createAggCurrencyStruct("value", "fiatValues", noFiatCurrencies.get)
          .as("TotalSpent")
      )
    val outStatsRelations = relations
      .groupBy("srcAddressId")
      .agg(
        count("transactionId").cast(IntegerType).as("noOutgoingTxs"),
        countDistinct("dstAddressId").cast(IntegerType).as("outDegree")
      )

    val outStatsToken = encodedTokenTransfers
      .groupBy("srcAddressId", "currency")
      .agg(
        TransformHelpers
          .createAggCurrencyStructPerCurrency(
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

    val outStats = outStatsEth
      .join(outStatsRelations, Seq("srcAddressId"), "full")
      .join(outStatsToken, Seq("srcAddressId"), "full")

    val inStatsEth = encodedTransactions
      .groupBy("dstAddressId")
      .agg(
        TransformHelpers
          .createAggCurrencyStruct("value", "fiatValues", noFiatCurrencies.get)
          .as("TotalReceived")
      )

    val inStatsRelations = relations
      .groupBy("dstAddressId")
      .agg(
        count("transactionId").cast(IntegerType).as("noIncomingTxs"),
        countDistinct("srcAddressId").cast(IntegerType).as("inDegree")
      )

    val inStatsToken = encodedTokenTransfers
      .groupBy("dstAddressId", "currency")
      .agg(
        TransformHelpers
          .createAggCurrencyStructPerCurrency(
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

    val inStats = inStatsEth
      .join(inStatsRelations, Seq("dstAddressId"), "full")
      .join(inStatsToken, Seq("dstAddressId"), "full")

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
      .transform(
        TransformHelpers.zeroValueIfNull(
          "totalReceived",
          noFiatCurrencies.get,
          castValueTo = DecimalType(38, 0)
        )
      )
      .transform(
        TransformHelpers.zeroValueIfNull(
          "totalSpent",
          noFiatCurrencies.get,
          castValueTo = DecimalType(38, 0)
        )
      )
      .join(addressIds, Seq("addressId"), "left")
      .transform(
        TransformHelpers.withIdGroup("addressId", "addressIdGroup", bucketSize)
      )
      .sort("addressId")
      .as[Address]
  }

  def computeAddressRelations(
      encodedTransactions: Dataset[EncodedTransaction],
      encodedTokenTransfers: Dataset[EncodedTokenTransfer]
  ): Dataset[AddressRelation] = {

    val aggValues = encodedTransactions.toDF.transform(
      TransformHelpers.aggregateValues(
        "value",
        "fiatValues",
        noFiatCurrencies.get,
        "srcAddressId",
        "dstAddressId"
      )
    )

    val aggValuesTokens = encodedTokenTransfers
      .groupBy("srcAddressId", "dstAddressId", "currency")
      .agg(
        TransformHelpers
          .createAggCurrencyStructPerCurrency(
            "value",
            "fiatValues",
            noFiatCurrencies.get
          )
          .as("tokenValues")
      )
      .groupBy("srcAddressId", "dstAddressId")
      .agg(
        map_from_entries(collect_set("tokenValues")).as("tokenValues")
      )

    val window = Window.partitionBy("srcAddressId", "dstAddressId")
    val outrelations = encodedTransactions
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
        aggValuesTokens,
        Seq("srcAddressId", "dstAddressId"),
        "left"
      )
      // add partitioning columns for outgoing addresses
      .transform(
        TransformHelpers
          .withIdGroup("srcAddressId", "srcAddressIdGroup", bucketSize)
      )
      .transform(
        TransformHelpers.withSecondaryIdGroup(
          "srcAddressIdGroup",
          "srcAddressIdSecondaryGroup",
          "srcAddressId"
        )
      )
      // add partitioning columns for incoming addresses
      .transform(
        TransformHelpers
          .withIdGroup("dstAddressId", "dstAddressIdGroup", bucketSize)
      )
      .transform(
        TransformHelpers.withSecondaryIdGroup(
          "dstAddressIdGroup",
          "dstAddressIdSecondaryGroup",
          "dstAddressId"
        )
      )
      .transform(
        TransformHelpers.zeroValueIfNull(
          "value",
          noFiatCurrencies.get,
          castValueTo = DecimalType(38, 0)
        )
      )

    /*    val withoutsrcgroup = outrelations
      .filter(col("srcAddressIdGroup").isNull)
    val withoutsrcgroupcnt = withoutsrcgroup.count()
    if (withoutsrcgroupcnt > 0) {
      println("Found address relations with null group: " + withoutsrcgroupcnt)
      println(withoutsrcgroup.show(100, false))
    }*/

    outrelations
      .filter(col("srcAddressIdGroup").isNotNull)
      .filter(col("dstAddressIdGroup").isNotNull)
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
