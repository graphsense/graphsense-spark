package org.graphsense.account.trx

import org.apache.spark.sql.functions.{col, max}
import org.graphsense.account.models.{
  AddressIdByAddressPrefix,
  TransactionIdByTransactionIdGroup,
  TransactionIdByTransactionPrefix
}
import org.graphsense.TestBase
import org.graphsense.TransformHelpers

class TransformationTest extends TestBase {
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  private val bucketSize = 2
  private val prefixLength = 4
  private val t = new TrxTransformation(spark, bucketSize)

  test("Check 0to10000 transform") {
    val inputDir = "src/test/resources/account/trx/0to10000/"
    inputDir + "reference/"
    val ds = new TestTrxSource(spark, inputDir)

    // read raw data
    val blocks = ds.blocks()
    val transactions = ds.transactions()
    val traces = ds.traces()
    val exchangeRatesRaw = ds.exchangeRates()
    val tokenConfigurations = ds.tokenConfigurations()
    val tokenTransfers = ds.tokenTransfers()

    // read ref values

    // Compute values
    blocks.count.toInt
    val lastBlockTimestamp = blocks
      .select(max(col("timestamp")))
      .first
      .getInt(0)
    transactions.count()

    val exchangeRates =
      t.computeExchangeRates(blocks, exchangeRatesRaw)
        .persist()

    val transactionIds = t.computeTransactionIds(transactions)
    transactionIds.toDF.transform(
      TransformHelpers.withSortedIdGroup[TransactionIdByTransactionIdGroup](
        "transactionId",
        "transactionIdGroup",
        bucketSize
      )
    )
    transactionIds.toDF.transform(
      TransformHelpers.withSortedPrefix[TransactionIdByTransactionPrefix](
        "transaction",
        "transactionPrefix",
        prefixLength
      )
    )
    val addressIds = t
      .computeAddressIds(traces, transactions, tokenTransfers)
      .sort("addressId")

    addressIds.toDF
      .transform(
        TransformHelpers.withSortedPrefix[AddressIdByAddressPrefix](
          "address",
          "addressPrefix",
          prefixLength
        )
      )
      .sort("addressId")

    val encodedTransactions =
      t.computeEncodedTransactions(
        traces,
        transactionIds,
        transactions,
        addressIds,
        exchangeRates
      ).persist()

    val encodedTokenTransfers =
      t.computeEncodedTokenTransfers(
        tokenTransfers,
        tokenConfigurations,
        transactionIds,
        addressIds,
        exchangeRates
      ).persist()

    t
      .computeBlockTransactions(blocks, encodedTransactions)
      .sort("blockId")

    val addressTransactions = t
      .computeAddressTransactions(
        encodedTransactions,
        encodedTokenTransfers
      )
      .persist()

    val contracts = t
      .computeContracts(
        traces,
        addressIds
      )
      .persist()

    t
      .computeAddresses(
        encodedTransactions,
        encodedTokenTransfers,
        addressTransactions,
        addressIds,
        contracts
      )
      .persist()

    val addressRelations = t
      .computeAddressRelations(encodedTransactions, encodedTokenTransfers)
      .sort("srcAddressId", "dstAddressId")

    note("Test lookup tables")

    note("Test blocks")

    note("Check statistics")
    assert(blocks.count.toInt == 10001, "expected 10001 blocks")
    assert(lastBlockTimestamp == 1529921544)
    assert(transactions.count() == 2172, "expected 2172 transaction")
    assert(addressIds.count() == 1772, "expected 1772 addresses")
    assert(addressRelations.count() == 1848, "expected 1848 address relations")

  }

  test("Check 56804500to56805500 transform") {
    val inputDir = "src/test/resources/account/trx/56804500to56805500/"
    inputDir + "reference/"
    val ds = new TestTrxSource(spark, inputDir)

    // read raw data
    val blocks = ds.blocks()
    val transactions = ds.transactions()
    val traces = ds.traces()
    val exchangeRatesRaw = ds.exchangeRates()
    val tokenConfigurations = ds.tokenConfigurations()
    val tokenTransfers = ds.tokenTransfers()

    // read ref values

    // Compute values
    blocks.count.toInt
    val lastBlockTimestamp = blocks
      .select(max(col("timestamp")))
      .first
      .getInt(0)
    transactions.count()

    val exchangeRates =
      t.computeExchangeRates(blocks, exchangeRatesRaw)
        .persist()

    val transactionIds = t.computeTransactionIds(transactions)
    transactionIds.toDF.transform(
      TransformHelpers.withSortedIdGroup[TransactionIdByTransactionIdGroup](
        "transactionId",
        "transactionIdGroup",
        bucketSize
      )
    )
    transactionIds.toDF.transform(
      TransformHelpers.withSortedPrefix[TransactionIdByTransactionPrefix](
        "transaction",
        "transactionPrefix",
        prefixLength
      )
    )
    val addressIds = t
      .computeAddressIds(traces, transactions, tokenTransfers)
      .sort("addressId")

    addressIds.toDF
      .transform(
        TransformHelpers.withSortedPrefix[AddressIdByAddressPrefix](
          "address",
          "addressPrefix",
          prefixLength
        )
      )
      .sort("addressId")

    val encodedTransactions =
      t.computeEncodedTransactions(
        traces,
        transactionIds,
        transactions,
        addressIds,
        exchangeRates
      ).persist()

    val encodedTokenTransfers =
      t.computeEncodedTokenTransfers(
        tokenTransfers,
        tokenConfigurations,
        transactionIds,
        addressIds,
        exchangeRates
      ).persist()

    t
      .computeBlockTransactions(blocks, encodedTransactions)
      .sort("blockId")

    val addressTransactions = t
      .computeAddressTransactions(
        encodedTransactions,
        encodedTokenTransfers
      )
      .persist()

    val contracts = t
      .computeContracts(
        traces,
        addressIds
      )
      .persist()

    t
      .computeAddresses(
        encodedTransactions,
        encodedTokenTransfers,
        addressTransactions,
        addressIds,
        contracts
      )
      .persist()

    val addressRelations = t
      .computeAddressRelations(encodedTransactions, encodedTokenTransfers)
      .sort("srcAddressId", "dstAddressId")

    note("Test lookup tables")

    note("Test blocks")

    note("Check statistics")
    assert(blocks.count.toInt == 1001, "expected 1001 blocks")
    assert(lastBlockTimestamp == 1701049164)
    assert(transactions.count() == 77090, "expected 77090 transaction")
    assert(addressIds.count() == 50702, "expected 50702 addresses")
    assert(
      addressRelations.count() == 77541,
      "expected 77541 address relations"
    )

  }

}
