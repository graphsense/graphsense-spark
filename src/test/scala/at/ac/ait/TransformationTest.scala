package at.ac.ait

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.{
  DataFrame,
  Dataset,
  Encoder,
  Encoders,
  SparkSession
}
import org.apache.spark.sql.catalyst.ScalaReflection.universe.TypeTag
import org.apache.spark.sql.functions.{col, length, lit, lower, max, udf}
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

  def readTestData[T <: Product: Encoder: TypeTag](file: String): Dataset[T] = {
    val schema = Encoders.product[T].schema
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
      .as[T]
  }

def setNullableStateForAllColumns[T](ds: Dataset[T], nullable: Boolean = true): DataFrame = {
    def set(st: StructType): StructType = {
      StructType(st.map {
        case StructField(name, dataType, _, metadata) =>
          val newDataType = dataType match {
            case t: StructType => set(t)
            case _ => dataType
          }
          StructField(name, newDataType, nullable = nullable, metadata)
      })
    }
    ds.sqlContext.createDataFrame(ds.toDF.rdd, set(ds.schema))
  }

  def assertDataFrameEquality[T](
      actualDS: Dataset[T],
      expectedDS: Dataset[T]
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
    readTestData[ExchangeRatesRaw](inputDir + "test_exchange_rates.json")
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
  val transactionIdsByTransactionIdGroup =
    transactionIds.toDF.transform(
      t.withSortedIdGroup[TransactionIdByTransactionIdGroup](
        "transactionId",
        "transactionIdGroup"
      )
    )
  val transactionIdsByTransactionPrefix =
    transactionIds.toDF.transform(
      t.withSortedPrefix[TransactionIdByTransactionPrefix](
        "transaction",
        "transactionPrefix"
      )
    )

  val addressIds = t.computeAddressIds(transactions)
  val addressIdsByAddressIdGroup =
    addressIds.toDF.transform(
      t.withSortedIdGroup[AddressIdByAddressIdGroup](
        "addressId",
        "addressIdGroup"
      )
    )
  val addressIdsByAddressPrefix =
    addressIds.toDF.transform(
      t.withSortedPrefix[AddressIdByAddressPrefix](
        "address",
        "addressPrefix"
      )
    )

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

  val addresses = t
    .computeAddresses(
      encodedTransactions,
      addressTransactions
    )
    .persist()

  val addressRelations =
    t.computeAddressRelations(
        encodedTransactions,
        addresses
      )
      .sort("srcAddressId", "dstAddressId")

  note("Test lookup tables")

  test("Transaction IDs") {
    val transactionIdsRef =
      readTestData[TransactionId](refDir + "transactions_ids.csv")
    assertDataFrameEquality(transactionIds, transactionIdsRef)
  }
  test("Transaction IDs by ID group") {
    val transactionIdsRef =
      readTestData[TransactionIdByTransactionIdGroup](
        refDir + "transactions_ids_by_id_group.csv"
      )
    assertDataFrameEquality(
      transactionIdsByTransactionIdGroup,
      transactionIdsRef
    )
  }
  test("Transaction IDs by transaction prefix") {
    val transactionIdsRef =
      readTestData[TransactionIdByTransactionPrefix](
        refDir + "transactions_ids_by_prefix.csv"
      )
    assertDataFrameEquality(
      transactionIdsByTransactionPrefix,
      transactionIdsRef
    )
  }

  test("Address IDs") {
    val addressIdsRef = readTestData[AddressId](refDir + "address_ids.csv")
    assertDataFrameEquality(addressIds, addressIdsRef)
  }

  test("Address IDs by ID Group") {
    val addressIdsRef = readTestData[AddressIdByAddressIdGroup](
      refDir + "address_ids_by_id_group.csv"
    )
    assertDataFrameEquality(addressIdsByAddressIdGroup, addressIdsRef)
  }

  test("Address IDs by address prefix") {
    val addressIdsRef = readTestData[AddressIdByAddressPrefix](
      refDir + "address_ids_by_prefix.csv"
    )
    assertDataFrameEquality(addressIdsByAddressPrefix, addressIdsRef)
  }

  note("Test exchange rates")

  test("Exchange rates") {
    val exchangeRatesRef =
      readTestData[ExchangeRates](refDir + "exchange_rates.json")
    assertDataFrameEquality(exchangeRates, exchangeRatesRef)
  }

  note("Test blocks")

  test("Block transactions") {
    val blockTransactionsRef =
      readTestData[BlockTransaction](refDir + "block_transactions.json")
    assertDataFrameEquality(blockTransactions, blockTransactionsRef)
  }

  note("Test address graph")

  test("Address transactions") {
    val addressTransactionsRef =
      readTestData[AddressTransaction](refDir + "address_transactions.csv")
    assertDataFrameEquality(addressTransactions, addressTransactionsRef)
  }

  test("Address tags") {
    val addressTagsRef =
      readTestData[AddressTag](refDir + "address_tags.csv")
    assertDataFrameEquality(addressTags, addressTagsRef)
  }

  test("Addresses") {
    val addressesRef =
      readTestData[Address](refDir + "addresses.json")
    assertDataFrameEquality(addresses, addressesRef)
  }

  test("Address relations") {
    val addressRelationsRef =
      readTestData[AddressRelation](refDir + "address_relations.json")
    assertDataFrameEquality(addressRelations, addressRelationsRef)
  }
}
