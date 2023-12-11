package org.graphsense.account.eth
import org.graphsense.TestBase
import org.graphsense.Util._

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

}
