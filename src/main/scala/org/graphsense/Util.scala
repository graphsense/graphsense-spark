package org.graphsense

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.Dataset

/*import org.apache.spark.sql.functions.{col, when}*/

object Util {

  import java.util.concurrent.TimeUnit

  def printDatasetStats[T](df: Dataset[T], name: String) = {
    df.explain()
    printStat(name ++ " partitions", df.rdd.getNumPartitions)
    printStat(
      name ++ " is persisted",
      df.storageLevel.useMemory || df.storageLevel.useDisk
    )
    printStat(name ++ " use memory", df.storageLevel.useMemory)
    printStat(name ++ " use disk", df.storageLevel.useDisk)
  }

  def computeMonotonicTxId(block: Int, positionInBlock: Int): Long = {
    ((block.toLong) << 32) | (positionInBlock & 0xffffffffL);
  }

  def decomposeMontonicTxId(txId: Long): Tuple2[Int, Int] = {
    ((txId >> 32).toInt, (txId).toInt)
  }

  val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def printStat[R](name: String, value: R): Unit = {
    println(f"${name}%-40s: ${value}%40s")
  }

  def time[R](title: String)(block: => R): R = {
    val timestamp = LocalDateTime.now().format(dtf)
    println(s"Start [${timestamp}] - ${title}")

    val (r, elapsedTime) = time(block)

    val timestampDone = LocalDateTime.now().format(dtf)
    val hour = TimeUnit.MILLISECONDS.toHours(elapsedTime)
    val min = TimeUnit.MILLISECONDS.toMinutes(elapsedTime) % 60
    val sec = TimeUnit.MILLISECONDS.toSeconds(elapsedTime) % 60

    println(
      s"Done  [${timestampDone}] - ${title} took ${"%02d:%02d:%02d".format(hour, min, sec)}"
    )

    return r
  }

  def time[R](block: => R): Tuple2[R, Long] = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()
    (result, t1 - t0)
  }

  def toIntSafe(value: Long): Int = {
    if (value.isValidInt) {
      return value.toInt
    } else {
      throw new ArithmeticException(f"${value} is out of an integers range.")
    }
  }

}
