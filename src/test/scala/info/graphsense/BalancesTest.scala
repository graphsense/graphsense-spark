package info.graphsense

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions.{col}
import org.scalatest.funsuite.AnyFunSuite

import Helpers.{readTestData, setNullableStateForAllColumns}

class BalancesTest
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

  private val inDir = "src/test/resources/"

  private val t = new Transformation(spark, 2)

  import spark.implicits._

  test("with mining activities and two unsuccessful transactions") {
    // two transactions did not succeed (signalled by invalid calltype, or status 0)

    val blocks =
      readTestData[Block](spark, inDir + "balance_blocks_with_miner.csv")
    val tx =
      readTestData[Transaction](spark, inDir + "test_transactions_complex.csv")
    val traces = readTestData[Trace](spark, inDir + "balance_traces.csv")
    val receipts = readTestData[Receipt](spark, inDir + "receipts.csv")

    val addressIds = t.computeAddressIds(traces)
    val balances =
      t.computeBalances(
          blocks,
          tx,
          traces,
          receipts,
          addressIds
        )
        .sort(col("addressId"))
    val expected =
      readTestData[Balance](spark, inDir + "reference_complex/balances.csv")

    assertDataFrameEquality(balances, expected)
  }

}
