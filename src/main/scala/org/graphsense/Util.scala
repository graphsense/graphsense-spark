package org.graphsense

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/*import org.apache.spark.sql.functions.{col, when}*/

object Util {

  import java.util.concurrent.TimeUnit

  val dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def printStat[R](name: String, value: R): Unit = {
    println(f"${name}%-20s: ${value}%40s")
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

}
