package org.graphsense.account.eth

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, forall, lit}
import org.graphsense.account.Implicits._
import org.graphsense.account.models.{Address, AddressRelation, TokenTransfer}
import org.graphsense.TestBase

class TokenTest extends TestBase {
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  private val inputDir = "src/test/resources/account/eth/tokens/"

  private val ds = new TestEthSource(spark, inputDir)
  private val t = new EthTransformation(spark, 2, 100000)

  test("full transform with logs") {
    // Parse all but only keep USDT token transfers for compare but parse all of them
    val transfers = ds
      .tokenTransfers()
      .filter(
        col("tokenAddress") === lit(
          hexStrToBytes("0xdAC17F958D2ee523a2206206994597C13D831ec7")
        )
      )

    val transfersRef =
      readTestData[TokenTransfer](
        inputDir + "/reference/USDT_token_transfers.csv"
      )

    assertDataFrameEquality(transfers, transfersRef)

  }

  test("encoded token transfers test") {
    // load raw data
    val blocks = ds.blocks()
    val transactions = ds.transactions()

    val traces = ds.traces()
    val exchangeRatesRaw = ds.exchangeRates()

    // parse all but only keep USDT token transfers for compare but parse all of them
    val transfers = ds.tokenTransfers()
    val tokenConfigs = ds.tokenConfigurations()

    // load ref data

    val addressesRef =
      readTestDataBase64[Address](
        inputDir + "/reference/addresses.json"
      )

    val addressRelationsRef =
      readTestDataBase64[AddressRelation](
        inputDir + "/reference/address_relations.json"
      )

    // compute data
    val exchangeRates =
      t.computeExchangeRates(blocks, exchangeRatesRaw)
        .persist()

    val transactionIds = t.computeTransactionIds(transactions)

    val addressIds = t
      .computeAddressIds(traces, transfers)
      .sort("addressId")

    val contracts = t.computeContracts(traces, addressIds)

    val encodedTransactions =
      t.computeEncodedTransactions(
        traces,
        transactionIds,
        addressIds,
        exchangeRates
      )

    val blockTransactions = t
      .computeBlockTransactions(encodedTransactions)
      .sort("blockId")

    val encodedTokenTransfers =
      t.computeEncodedTokenTransfers(
        transfers,
        tokenConfigs,
        transactionIds,
        addressIds,
        exchangeRates
      ).persist()

    val addressRelations =
      t.computeAddressRelations(encodedTransactions, encodedTokenTransfers)

    val addressTransactions = t
      .computeAddressTransactions(
        encodedTransactions,
        encodedTokenTransfers
      )

    val addresses = t
      .computeAddresses(
        encodedTransactions,
        encodedTokenTransfers,
        addressTransactions,
        addressIds,
        contracts
      )

    // check stuff

    assert(
      blockTransactions
        .count() === blockTransactions.dropDuplicates("txId").count(),
      "duplicates in block transactions"
    )

    assert(
      encodedTransactions
        .withColumn(
          "allfiatset",
          forall(col("fiatValues"), (colmn: Column) => colmn.isNotNull)
        )
        .filter(col("allfiatset") === lit(false))
        .count() === 0
    )

    assert(
      encodedTransactions.filter(col("dstAddressId").isNull).count() === 0
    )

    assert(
      encodedTransactions.filter(col("dstAddressId").isNull).count() === 0
    )

    assert(
      encodedTransactions
        .filter(col("dstAddressId").isNull)
        .count() === transactions.filter(col("toAddress").isNull).count()
    )

    assert(
      encodedTokenTransfers
        .withColumn(
          "allfiatset",
          forall(col("fiatValues"), (colmn: Column) => colmn.isNotNull)
        )
        .filter(col("allfiatset") === lit(false))
        .count() === 0
    )

    assert(
      encodedTokenTransfers.filter(col("srcAddressId").isNull).count() === 0
    )

    assert(
      encodedTokenTransfers.filter(col("dstAddressId").isNull).count() === 0
    )

    assert(
      addressTransactions
        .filter(col("transactionId").isNull)
        .count() === 0
    )

    assert(
      addresses
        .filter(col("totalTokensSpent").isNotNull)
        .filter(col("totalSpent.value") > 0)
        .count() === 31
    )

    assert(
      addresses
        .filter(col("isContract") === true)
        .count() === 3
    )

    assert(
      addresses
        .filter(col("noIncomingTxs") === 0 && col("noOutgoingTxs") === 0)
        .count() === 0
    )

    // this is currently not true since for the address transactions, tx table is used not traces

    /*    val address_count = addressIds.count()
        assert(addresses.count() === address_count)*/

    assert(
      addresses
        .count() === addresses.select(col("addressId")).distinct().count()
    )

    assert(
      addresses.count() === addresses.select(col("address")).distinct().count()
    )

    // addresses.write.format("json").mode("overwrite").save("addresses.json")

    // addressRelations
    //  .toDF()
    //  .write
    //  .format("json")
    //  .mode("overwrite")
    //  .save("addressRelations.json")

    assertDataFrameEqualityGeneric(
      addresses,
      addressesRef,
      ignoreCols = List(
        "noIncomingTxsZeroValue",
        "noOutgoingTxsZeroValue",
        "inDegreeZeroValue",
        "outDegreeZeroValue"
      )
    )

    assert(
      addressRelations
        .filter(col("tokenValues").isNotNull)
        .filter(col("value.value") > 0)
        .count() === 1
    )
    assert(addressRelations.filter(col("srcAddressId").isNull).count() === 0)
    assert(addressRelations.filter(col("dstAddressId").isNull).count() === 0)
    assert(
      addressRelations
        .filter(col("tokenValues").isNotNull)
        .filter(col("value.value") > 0)
        .count() === 1
    )

    assertDataFrameEquality(addressRelations, addressRelationsRef)

  }
}
