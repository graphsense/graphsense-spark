package info.graphsense

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import info.graphsense.Helpers.readTestData
import org.scalatest.funsuite.AnyFunSuite

class BalancesTest extends AnyFunSuite
  with SparkSessionTestWrapper  with DataFrameComparer{

  private val inDir = "src/test/resources/"

  private val t = new Transformation(spark, 2)

  import spark.implicits._

  test("with mining activities and two unsuccessful transactions") {
    // two transactions did not succeed (signalled by invalid calltype, or status 0)

    val genesis = readTestData[GenesisTransfer](spark, inDir + "genesis_transfers.csv")
    val blocks = readTestData[Block](spark, inDir + "balance_blocks_with_miner.csv")
    val tx = readTestData[Transaction](spark, inDir + "test_transactions_complex.csv")
    val traces = readTestData[BalanceTrace](spark, inDir + "balance_traces.csv")
    val receipts = readTestData[Receipt](spark, inDir + "receipts.csv")

    val balances = t.computeBalances(genesis, blocks, tx, traces, receipts)
    val expected = readTestData[Balances](spark, inDir + "reference_complex/balances.csv")

    assertSmallDataFrameEquality(balances.toDF(), expected.toDF(), orderedComparison = false)
  }

}
