package at.ac.ait

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lower}
import org.rogach.scallop._

import at.ac.ait.storage._

object TransformationJob {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val rawKeyspace: ScallopOption[String] =
      opt[String](
        "raw-keyspace",
        required = true,
        noshort = true,
        descr = "Raw keyspace"
      )
    val tagKeyspace: ScallopOption[String] =
      opt[String](
        "tag-keyspace",
        required = true,
        noshort = true,
        descr = "Tag keyspace"
      )
    val targetKeyspace: ScallopOption[String] = opt[String](
      "target-keyspace",
      required = true,
      noshort = true,
      descr = "Transformed keyspace"
    )
    val bucketSize: ScallopOption[Int] = opt[Int](
      "bucket-size",
      required = false,
      default = Some(25000),
      noshort = true,
      descr = "Bucket size for Cassandra partitions"
    )
    verify()
  }

  def main(args: Array[String]) {

    val conf = new Conf(args)

    val spark = SparkSession.builder
      .appName("GraphSense Transformation [%s]".format(conf.targetKeyspace()))
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    println("Raw keyspace:                  " + conf.rawKeyspace())
    println("Tag keyspace:                  " + conf.tagKeyspace())
    println("Target keyspace:               " + conf.targetKeyspace())
    println("Bucket size:                   " + conf.bucketSize())

    import spark.implicits._

    val cassandra = new CassandraStorage(spark)

    val exchangeRatesRaw =
      cassandra.load[ExchangeRatesRaw](conf.rawKeyspace(), "exchange_rates")
    exchangeRatesRaw.show(5) // TODO

    val blocks =
      cassandra.load[Block](conf.rawKeyspace(), "block")
    val transactions =
      cassandra.load[Transaction](conf.rawKeyspace(), "transaction")
    val tagsRaw = cassandra
      .load[TagRaw](conf.tagKeyspace(), "tag_by_address")

    println("blocks")
    val noBlocks = blocks.count()
    blocks.show(2)
    blocks.printSchema
    println("Number of blocks: " + noBlocks)
    val lastBlockTimestamp = blocks
      .filter(col("number") === noBlocks - 1)
      .select(col("timestamp"))
      .first()
      .getInt(0)
    println("Last timestamp: " + lastBlockTimestamp)
    println("txs")
    transactions.show(2)
    transactions.printSchema
    val noTransactions = transactions.count()
    println("Number of transactions: " + noTransactions)

    val transformation = new Transformation(spark, conf.bucketSize())

    //println("Store configuration")
    //val configuration =
    //  transformation.configuration(
    //    conf.targetKeyspace(),
    //    conf.bucketSize(),
    //  )
    //cassandra.store(
    //  conf.targetKeyspace(),
    //  "configuration",
    //  configuration
    //)

    println("Computing exchange rates")
    val exchangeRates =
      transformation
        .computeExchangeRates(blocks, exchangeRatesRaw)
        .persist()
    cassandra.store(conf.targetKeyspace(), "exchange_rates", exchangeRates)

    val transactionIds =
      transformation.computeTransactionIds(transactions).persist()
    val transactionIdsByTransactionIdGroup =
      transactionIds.toDF.transform(
        transformation.withSortedIdGroup[TransactionIdByTransactionIdGroup](
          "transactionId",
          "transactionIdGroup"
        )
      )
    cassandra.store(
      conf.targetKeyspace(),
      "transaction_ids_by_transaction_id_group",
      transactionIdsByTransactionIdGroup
    )
    val transactionIdsByTransactionPrefix =
      transactionIds.toDF.transform(
        transformation.withSortedPrefix[TransactionIdByTransactionPrefix](
          "transaction",
          "transactionPrefix"
        )
      )
    cassandra.store(
      conf.targetKeyspace(),
      "transaction_ids_by_transaction_prefix",
      transactionIdsByTransactionPrefix
    )

    val addressIds = transformation.computeAddressIds(transactions)
    val addressIdsByAddressIdGroup =
      addressIds.toDF.transform(
        transformation.withSortedIdGroup[AddressIdByAddressIdGroup](
          "addressId",
          "addressIdGroup"
        )
      )
    cassandra.store(
      conf.targetKeyspace(),
      "address_ids_by_address_id_group",
      addressIdsByAddressIdGroup
    )
    val addressIdsByAddressPrefix =
      addressIds.toDF.transform(
        transformation.withSortedPrefix[AddressIdByAddressPrefix](
          "address",
          "addressPrefix"
        )
      )
    cassandra.store(
      conf.targetKeyspace(),
      "address_ids_by_address_prefix",
      addressIdsByAddressPrefix
    )

    println("Encoding transactions")
    val encodedTransactions =
      transformation
        .computeEncodedTransactions(
          transactions,
          transactionIds,
          addressIds,
          exchangeRates
        )
        .persist()
    encodedTransactions.show(5)
    encodedTransactions
      .sort("transactionId")
      .filter(col("transactionId") < 10)
      .show()

    println("Computing block transactions")
    val blockTransactions = transformation
      .computeBlockTransactions(blocks, encodedTransactions)
    blockTransactions.filter($"height" > 46990).sort("height").show()

    println("Computing address transactions")
    val addressTransactions = transformation
      .computeAddressTransactions(encodedTransactions)
      .persist()
    addressTransactions.show(25)
    println(addressTransactions.count)

    println("Computing address tags")
    val addressTags =
      transformation
        .computeAddressTags(
          tagsRaw,
          addressIds,
          "ETH"
        )
        .persist()
    //cassandra.store(conf.targetKeyspace(), "address_tags", addressTags)
    val noAddressTags = addressTags
      .select(col("label"))
      .withColumn("label", lower(col("label")))
      .distinct()
      .count()
    println("Number of tags: " + noAddressTags)
    addressTags.show()

    val addresses = transformation.computeAddresses(
      encodedTransactions,
      addressTransactions
    )
    addresses.show(20)

    val addressRelations =
      transformation.computeAddressRelations(
        encodedTransactions,
        addresses
      )
    addressRelations.show(20, false)

    //cassandra.store(conf.targetKeyspace(), "block_transactions", blockTransactions)
    /*
    println("Computing address IDs")
    val addressIds =
      transformation.computeAddressIds(regOutputs)

    cassandra.store(
      conf.targetKeyspace(),
      "address_by_id_group",
      addressByIdGroup
    )

    cassandra.store(
      conf.targetKeyspace(),
      "address_transactions",
      addressTransactions
    )

    println("Computing summary statistics")
    val summaryStatistics =
      transformation.summaryStatistics(
        lastBlockTimestamp,
        noBlocks,
        noTransactions,
        noAddresses,
        noAddressRelations,
        noAddressTags
      )
    summaryStatistics.show()
    cassandra.store(
      conf.targetKeyspace(),
      "summary_statistics",
      summaryStatistics
    )
     */
    spark.stop()
  }
}
