package info.graphsense

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions.{col, max}
import org.scalatest.funsuite.AnyFunSuite

import Helpers.{readTestData, setNullableStateForAllColumns}
/*
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

  private val inputDir = "src/test/resources/complex_graph/"
  private val refDir = inputDir + "reference/"

  val txs =
    readTestData[Transaction](spark, inputDir + "test_transactions.csv")
  val traces = readTestData[Trace](spark, inputDir + "test_traces.csv")
  val blocks =
    readTestData[Block](spark, inputDir + "test_blocks.csv")

  val exchangeRatesRaw =
    readTestData[ExchangeRatesRaw](spark, inputDir + "test_exchange_rates.json")

  val tokenTransfers = spark.emptyDataset[TokenTransfer]
  val encodedTokenTransfers = spark.emptyDataset[EncodedTokenTransfer]
  val contracts = spark.emptyDataset[Contract]

  // transformation pipeline

  private val t = new Transformation(spark, 2)

  val exchangeRates =
    t.computeExchangeRates(blocks, exchangeRatesRaw).persist()
  val txIds = t.computeTransactionIds(txs)
  val addressIds =
    t.computeAddressIds(traces, tokenTransfers)
  val encodedTxs =
    t.computeEncodedTransactions(traces, txIds, addressIds, exchangeRates)
  val addressTransactions = t.computeAddressTransactions(
    encodedTxs,
    encodedTokenTransfers
  )
  val addresses =
    t.computeAddresses(
      encodedTxs,
      encodedTokenTransfers,
      addressTransactions,
      addressIds,
      contracts
    ).persist()
  val addressRelations = t
    .computeAddressRelations(encodedTxs, encodedTokenTransfers)
    .sort("srcAddressId", "dstAddressId")
  val lastBlockTimestamp = blocks
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
*/