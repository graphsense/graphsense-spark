package info.graphsense

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions.{col, max}
import org.scalatest.funsuite.AnyFunSuite

import Helpers.{readTestData, setNullableStateForAllColumns}

class ComplexGraphTransformationTest
    extends AnyFunSuite
    with SparkSessionTestWrapper
    with DataFrameComparer {

  def assertDataFrameEquality[T](
      actualDS: Dataset[T],
      expectedDS: Dataset[T]
  ): Unit = {
    val colOrder: Array[Column] = expectedDS.columns.map(col)

    assertSmallDataFrameEquality(
      setNullableStateForAllColumns(actualDS.select(colOrder: _*)),
      setNullableStateForAllColumns(expectedDS)
    )
  }

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  private val inDir = "src/test/resources/"
  private val refDir = "src/test/resources/reference_complex/"

  private val txs = readTestData[Transaction](
    spark,
    inDir + "test_transactions_complex.csv"
  )
  private val traces = readTestData[Trace](spark, inDir + "test_traces.csv")
  private val blocks = readTestData[Block](spark, inDir + "balance_blocks_with_miner.csv")

  private val exchangeRatesRaw =
    readTestData[ExchangeRatesRaw](spark, inDir + "test_exchange_rates.json")

  private val bucketSize = 2

  private val t = new Transformation(spark, bucketSize)

  private val exchangeRates =
    t.computeExchangeRates(blocks, exchangeRatesRaw).persist()
  private val txIds = t.computeTransactionIds(txs)
  private val addressIds = t.computeAddressIds(traces)
  private val encodedTxs =
    t.computeEncodedTransactions(txs, txIds, addressIds, exchangeRates)
  private val addressTransactions = t.computeAddressTransactions(encodedTxs)
  private val addresses =
    t.computeAddresses(encodedTxs, addressTransactions, addressIds).persist()
  private val addressTags = spark.emptyDataset[AddressTag]
  private val addressRelations = t
    .computeAddressRelations(encodedTxs, addressTags)
    .sort("srcAddressId", "dstAddressId")
  private val lastBlockTimestamp = blocks
    .select(max(col("timestamp")))
    .first
    .getInt(0)

  note("Testing address graph:")
  test("Address transactions") {
    val addressTransactionsRef =
      readTestData[AddressTransaction](
        spark,
        refDir + "address_transactions.csv"
      )
    assertDataFrameEquality(addressTransactions, addressTransactionsRef)
  }

  test("Addresses") {
    val addressesRef =
      readTestData[Address](spark, refDir + "addresses.json")
    assertDataFrameEquality(addresses, addressesRef)
  }

  test("Address relations") {
    val addressRelationsRef =
      readTestData[AddressRelation](spark, refDir + "address_relations.json")
    assertDataFrameEquality(addressRelations, addressRelationsRef)
  }

  test("Check statistics") {
    assert(blocks.count.toInt == 10, "expected 10 blocks")
    assert(lastBlockTimestamp == 1438919571)
    assert(txs.count() == 10, "expected 10 transaction")
    assert(addressIds.count() == 59, "expected 59 addresses")
    assert(addressRelations.count() == 9, "expected 9 address relations")
  }

}
