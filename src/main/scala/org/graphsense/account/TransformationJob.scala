package org.graphsense.account

import org.apache.spark.sql.SparkSession
import org.graphsense.Job
import org.graphsense.account.config.AccountConfig
import org.graphsense.account.eth.{CassandraEthSource, EthereumJob}
import org.graphsense.account.trx.{CassandraTrxSource, TronJob}
import org.graphsense.storage.CassandraStorage

object TransformationJob {

  def main(args: Array[String]): Unit = {
    // import spark.implicits._

    val conf = new AccountConfig(args)

    val spark = SparkSession.builder
      .appName("GraphSense Transformation [%s]".format(conf.targetKeyspace()))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    println("Raw keyspace:                  " + conf.rawKeyspace())
    println("Target keyspace:               " + conf.targetKeyspace())
    println("Bucket size:                   " + conf.bucketSize())
    println("Address prefix length:         " + conf.addressPrefixLength())
    println("Tx prefix length:              " + conf.txPrefixLength())
    println(
      "Min block:                     " + conf.minBlock.toOption.getOrElse(-1)
    )
    println(
      "Max block:                     " + conf.maxBlock.toOption.getOrElse(-1)
    )
    println(
      "Cache dataset dir:             " + conf.cacheDirectory.toOption
        .getOrElse("not set")
    )
    println(
      "Debug level:                   " + conf.debug.toOption
        .getOrElse(0)
    )

    val cassandra = new CassandraStorage(spark)

    val transform: Job = conf.network() match {
      case "eth" =>
        new EthereumJob(
          spark,
          new CassandraEthSource(
            cassandra,
            conf.rawKeyspace()
          ),
          new CassandraAccountSink(cassandra, conf.targetKeyspace()),
          conf
        )
      case "trx" =>
        new TronJob(
          spark,
          new CassandraTrxSource(cassandra, conf.rawKeyspace()),
          new CassandraAccountSink(cassandra, conf.targetKeyspace()),
          conf
        )
      case _ =>
        throw new IllegalArgumentException(
          "Transformation for given network not found."
        )
    }

    transform.run(None, None)

    spark.stop()
  }
}
