package info.graphsense

import scala.language.implicitConversions

/*import org.apache.spark.sql.functions.{col, when}*/

object Util {

  import java.util.concurrent.TimeUnit

  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()

    val elapsed_time = t1 - t0
    val hour = TimeUnit.MILLISECONDS.toHours(elapsed_time)
    val min = TimeUnit.MILLISECONDS.toMinutes(elapsed_time) % 60
    val sec = TimeUnit.MILLISECONDS.toSeconds(elapsed_time) % 60
    println("Time: %02d:%02d:%02d".format(hour, min, sec))
    result
  }

}

object Conversion {
  implicit def hexstr_to_bytes(s: String): Array[Byte] = {
    val hex = canonical_hex(s)
    assert(hex.length % 2 == 0, "Hex string has wrong length")
    hex.sliding(2, 2).map(Integer.parseInt(_, 16).toByte).toArray
  }

  def canonical_hex(s: String): String = {
    s.stripPrefix("0x").toLowerCase()
  }

  implicit def hexstr_to_bi(s: String): BigInt = {
    BigInt(canonical_hex(s), 16)
  }

  implicit def bytes_to_hexstr(bytes: Array[Byte]): String = {
    val sb = new StringBuilder
    for (b <- bytes) {
      sb.append(String.format("%02x", Byte.box(b)))
    }
    "0x" + sb.toString
  }

  def bytes_to_hexstr_can(bytes: Array[Byte]): String = {
    canonical_hex(bytes_to_hexstr(bytes))
  }

}
