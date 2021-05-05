package at.ac.ait

import at.ac.ait.Helpers.{readTestData, setNullableStateForAllColumns}
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, lower, max}
import org.scalatest.funsuite._

class TransformationTest
    extends AnyFunSuite
    with SparkSessionTestWrapper
    with DataFrameComparer {

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
  val prefixLength: Int = 4

  // input data
  val blocks = readTestData[Block](spark, inputDir + "test_blocks.csv")
  val transactions =
    readTestData[Transaction](spark, inputDir + "test_transactions.csv")
  val exchangeRatesRaw =
    readTestData[ExchangeRatesRaw](spark, inputDir + "test_exchange_rates.json")
  val attributionTags =
    readTestData[AddressTagRaw](spark, inputDir + "test_tags.json")

  val noBlocks = blocks.count.toInt
  val lastBlockTimestamp = blocks
    .select(max(col("timestamp")))
    .first
    .getInt(0)
  val noTransactions = transactions.count()

  // transformation pipeline

  val t = new Transformation(spark, bucketSize, prefixLength)

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
      .sort("height")
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
        addresses,
        addressTags
      )
      .sort("srcAddressId", "dstAddressId")

  val addressRelationsTxLimit1 =
    t.computeAddressRelations(
        encodedTransactions,
        addresses,
        addressTags,
        1
      )
      .sort("srcAddressId", "dstAddressId")

  note("Test lookup tables")

  test("Transaction IDs") {
    val transactionIdsRef =
      readTestData[TransactionId](spark, refDir + "transactions_ids.csv")
    assertDataFrameEquality(transactionIds, transactionIdsRef)
  }
  test("Transaction IDs by ID group") {
    val transactionIdsRef =
      readTestData[TransactionIdByTransactionIdGroup](
        spark,
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
        spark,
        refDir + "transactions_ids_by_prefix.csv"
      )
    assertDataFrameEquality(
      transactionIdsByTransactionPrefix,
      transactionIdsRef
    )
  }

  test("Address IDs") {
    val addressIdsRef =
      readTestData[AddressId](spark, refDir + "address_ids.csv")
    assertDataFrameEquality(addressIds, addressIdsRef)
  }

  test("Address IDs by ID Group") {
    val addressIdsRef = readTestData[AddressIdByAddressIdGroup](
      spark,
      refDir + "address_ids_by_id_group.csv"
    )
    assertDataFrameEquality(addressIdsByAddressIdGroup, addressIdsRef)
  }

  test("Address IDs by address prefix") {
    val addressIdsRef = readTestData[AddressIdByAddressPrefix](
      spark,
      refDir + "address_ids_by_prefix.csv"
    )
    assertDataFrameEquality(addressIdsByAddressPrefix, addressIdsRef)
  }

  note("Test exchange rates")

  test("Exchange rates") {
    val exchangeRatesRef =
      readTestData[ExchangeRates](spark, refDir + "exchange_rates.json")
    assertDataFrameEquality(exchangeRates, exchangeRatesRef)
  }

  test("Encoded transactions") {
    val encTransactionsRef =
      readTestData[EncodedTransaction](
        spark,
        refDir + "encoded_transactions.json"
      )
    assertDataFrameEquality(encodedTransactions, encTransactionsRef)
  }

  note("Test blocks")

  test("Block transactions") {
    val blockTransactionsRef =
      readTestData[BlockTransaction](spark, refDir + "block_transactions.json")
    assertDataFrameEquality(blockTransactions, blockTransactionsRef)
  }

  note("Test address graph")

  test("Address transactions") {
    val addressTransactionsRef =
      readTestData[AddressTransaction](
        spark,
        refDir + "address_transactions.csv"
      )
    assertDataFrameEquality(addressTransactions, addressTransactionsRef)
  }

  test("Address tags") {
    val addressTagsRef =
      readTestData[AddressTag](spark, refDir + "address_tags.csv")
    assertDataFrameEquality(addressTags, addressTagsRef)
  }

  test("Addresses") {
    val addressesRef =
      readTestData[Address](spark, refDir + "addresses.json")
    assertDataFrameEquality(addresses, addressesRef)
  }

  test("Address relations") {
    val addressRelationsRef =
      readTestData[AddressRelation](spark, refDir + "address_relations.json")
    assertDataFrameEquality(addressRelations, addressRelationsRef)
  }

  test("Address relations with Tx Limit 1") {
    val addressRelationsRef =
      readTestData[AddressRelation](spark, refDir + "address_relations_txlimit1.json")
    assertDataFrameEquality(addressRelationsTxLimit1, addressRelationsRef)
  }

  test("Check statistics") {
    assert(blocks.count.toInt == 84, "expected 84 blocks")
    assert(lastBlockTimestamp == 1438919571)
    assert(transactions.count() == 10, "expected 10 transaction")
    assert(addressIds.count() == 15, "expected 15 addresses")
    assert(addressRelations.count() == 9, "expected 9 address relations")
  }
}
