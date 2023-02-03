package info.graphsense

import scala.language.implicitConversions

/*import org.apache.spark.sql.functions.{col, when}*/

object Util {

  import java.util.concurrent.TimeUnit

  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()

    val elapsedTime = t1 - t0
    val hour = TimeUnit.MILLISECONDS.toHours(elapsedTime)
    val min = TimeUnit.MILLISECONDS.toMinutes(elapsedTime) % 60
    val sec = TimeUnit.MILLISECONDS.toSeconds(elapsedTime) % 60
    println("Time: %02d:%02d:%02d".format(hour, min, sec))
    result
  }

}

object Conversion {
  implicit def hexStrToBytes(s: String): Array[Byte] = {
    val hex = canonicalHex(s)
    assert(hex.length % 2 == 0, "Hex string has wrong length")
    hex.sliding(2, 2).map(Integer.parseInt(_, 16).toByte).toArray
  }

  def canonicalHex(s: String): String = {
    s.stripPrefix("0x").toLowerCase()
  }

  implicit def hexstrToBigInt(s: String): BigInt = {
    BigInt(canonicalHex(s), 16)
  }

  implicit def bytesToHexStr(bytes: Array[Byte]): String = {
    val sb = new StringBuilder
    for (b <- bytes) {
      sb.append(String.format("%02x", Byte.box(b)))
    }
    "0x" + sb.toString
  }

  def bytesToHexStrCan(bytes: Array[Byte]): String = {
    canonicalHex(bytesToHexStr(bytes))
  }

}
