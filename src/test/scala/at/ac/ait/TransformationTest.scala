package at.ac.ait

import com.github.mrpowers.spark.fast.tests.{DataFrameComparer}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag
import org.apache.spark.sql.functions.{col, hex, length, lit, lower, max, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite._

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("Transformation Test")
      .getOrCreate()
  }
}

class TransformationTest
    extends AnyFunSuite
    with SparkSessionTestWrapper
    with DataFrameComparer {

  def readTestData[A: Encoder: TypeTag](file: String): Dataset[A] = {
    // https://docs.databricks.com/spark/latest/faq/schema-from-case-class.html
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    // spark.read.csv cannot read BinaryTaype, read BinaryType as StringType and cast to ByteArray
    val newSchema = StructType(
      schema.map(
        x =>
          if (x.dataType.toString == "BinaryType")
            StructField(x.name, StringType, true)
          else StructField(x.name, x.dataType, true)
      )
    )
    val binaryColumns = schema.flatMap(
      x => if (x.dataType.toString == "BinaryType") Some(x.name) else None
    )
    val hexStringToByteArray = udf(
      (x: String) => x.grouped(2).toArray map { Integer.parseInt(_, 16).toByte }
    )

    val fileSuffix = file.toUpperCase.split("\\.").last
    val df =
      if (fileSuffix == "JSON") spark.read.schema(newSchema).json(file)
      else spark.read.schema(newSchema).option("header", true).csv(file)

    binaryColumns
      .foldLeft(df) { (curDF, colName) =>
        curDF.withColumn(
          colName,
          hexStringToByteArray(
            col(colName).substr(lit(3), length(col(colName)) - 2)
          )
        )
      }
      .as[A]
  }

  def setNullableStateForAllColumns[A](ds: Dataset[A]): DataFrame = {
    val df = ds.toDF()
    val schema = StructType(
      df.schema.map(
        x => StructField(x.name, x.dataType, true)
      )
    )
    df.sqlContext.createDataFrame(df.rdd, schema)
  }

  def assertDataFrameEquality[A](
      actualDS: Dataset[A],
      expectedDS: Dataset[A]
  ): Unit = {
    val colOrder = expectedDS.columns map col
    assertSmallDataFrameEquality(
      setNullableStateForAllColumns(actualDS.select(colOrder: _*)),
      setNullableStateForAllColumns(expectedDS)
    )
  }

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  val inputDir = "src/test/resources/"
  val refDir = "src/test/resources/reference/"

  val bucketSize: Int = 2

  // input data
  val blocks = readTestData[Block](inputDir + "test_blocks.csv")
  val transactions =
    readTestData[Transaction](inputDir + "test_transactions.csv")
  val exchangeRatesRaw =
    readTestData[ExchangeRatesRaw](inputDir + "test_exchange_rates.csv")
  val attributionTags = readTestData[TagRaw](inputDir + "test_tags.json")

  val noBlocks = blocks.count.toInt
  val lastBlockTimestamp = blocks
    .select(max(col("timestamp")))
    .first
    .getInt(0)
  val noTransactions = transactions.count()

  // transformation pipeline

  val t = new Transformation(spark, bucketSize)

  val exchangeRates =
    t.computeExchangeRates(blocks, exchangeRatesRaw)
      .persist()

  val transactionIds = t.computeTransactionIds(transactions)
  val addressIds = t.computeAddressIds(transactions)

  val encodedTransactions =
    t.computeEncodedTransactions(
        transactions,
        transactionIds,
        addressIds,
        exchangeRates
      )
      .persist()

  val blockTransactions = t
    .computeBlockTransactions(blocks, encodedTransactions)
    .sort("height")

  val addressTransactions = t
    .computeAddressTransactions(encodedTransactions)
    .persist()

  val addressTags =
    t.computeAddressTags(
      attributionTags,
      addressIds,
      "ETH"
    )

  val noAddressTags = addressTags
    .select(col("label"))
    .withColumn("label", lower(col("label")))
    .distinct()
    .count()

  val addresses = t.computeAddresses(
    encodedTransactions,
    addressTransactions
  )

  val addressRelations =
    t.computeAddressRelations(
      encodedTransactions,
      addresses
    )

  note("Test address graph")

  test("Transaction IDs") {
    val transactionIdsRef =
      readTestData[TransactionId](refDir + "transactions_ids.csv")
    assertDataFrameEquality(transactionIds, transactionIdsRef)
  }

  test("Address IDs") {
    val addressIdsRef = readTestData[AddressId](refDir + "address_ids.csv")
    assertDataFrameEquality(addressIds, addressIdsRef)
  }

  test("Block transactions") {
    val blockTransactionsRef =
      readTestData[BlockTransaction](refDir + "block_transactions.json")
    assertDataFrameEquality(blockTransactions, blockTransactionsRef)
  }
}
