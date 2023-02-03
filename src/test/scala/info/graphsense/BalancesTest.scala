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

  private val inputDir = "src/test/resources/balance/"
  private val refDir = inputDir + "reference/"

  private val t = new Transformation(spark, 2)
  private val tt = new TokenTransfers(spark)

  import spark.implicits._

  test("with mining activities and two unsuccessful transactions") {
    // two transactions did not succeed (signalled by invalid calltype, or status 0)

    val blocks = readTestData[Block](spark, inputDir + "balance_blocks.csv")
    val tx =
      readTestData[Transaction](spark, inputDir + "balance_transactions.csv")
    val traces = readTestData[Trace](spark, inputDir + "balance_traces.csv")
    val logs = readTestData[Log](spark, inputDir + "logs.json")

    val tokenConfigs = tt.getTokenConfigurations()
    val tokenSelection = tokenConfigs
      .filter(col("currencyTicker").isin(Array("USDT", "USDC", "WETH"): _*))
      .map(x => x.tokenAddress)
      .collect()

    val tokenTransfers = tt.getTokenTransfers(logs, tokenSelection)

    val addressIds = t.computeAddressIds(traces, tokenTransfers)
    val balances =
      t.computeBalances(
        blocks,
        tx,
        traces,
        addressIds,
        tokenTransfers,
        tokenConfigs
      ).sort(col("currency"), col("addressId"))

    val balancesRef =
      readTestData[Balance](spark, refDir + "balances.csv")

    assertDataFrameEquality(balances, balancesRef)
  }
}
