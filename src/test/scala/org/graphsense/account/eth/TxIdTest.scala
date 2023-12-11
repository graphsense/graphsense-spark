package org.graphsense.account.eth
import org.graphsense.TestBase
import org.graphsense.Util._
import org.apache.spark.SparkException

class TxIdTest extends TestBase {

  test("Monotonic txid test") {
    val blks = Range.inclusive(1250000000, 1250010000)
    val index = Range.inclusive(10000000, 10001000)

    var last = 0L;
    for (n <- blks) {
      for (i <- index) {
        val monotonictx = computeMonotonicTxId(n, i)
        assert(last < monotonictx)
        val (v1, v2) = decomposeMontonicTxId(monotonictx)
        assert(v1 == n)
        assert(v2 == i)
        last = monotonictx
      }
    }

  }

  test("Decimal(38,0) overflow behavior") {
    import spark.implicits._
    val data = Seq(("Java", BigInt(10).pow(37), BigInt(10).pow(37)))
    val rdd = spark.sparkContext.parallelize(data)
    val df = rdd.toDF("name", "a", "b")

    assertThrows[SparkException] {

      spark.conf.set("spark.sql.ansi.enabled", true)

      df.withColumn("c", $"a" * $"b").show()

    }

    spark.conf.set("spark.sql.ansi.enabled", false)

    df
  }

}
