package org.graphsense.account.eth

import org.apache.spark.sql.functions.{array_distinct, col, max, size}
import org.graphsense.account.models.{
  Address,
  AddressId,
  AddressIdByAddressPrefix,
  AddressRelation,
  AddressTransaction,
  BlockTransaction,
  Contract,
  EncodedTokenTransfer,
  EncodedTransaction,
  TokenTransfer,
  TransactionId,
  TransactionIdByTransactionIdGroup,
  TransactionIdByTransactionPrefix
}
import org.graphsense.TestBase
import org.graphsense.TransformHelpers
import org.graphsense.models.ExchangeRates

class TransformationTest extends TestBase {
  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  private val inputDir = "src/test/resources/account/eth/simple_graph/"
  private val refDir = inputDir + "reference/"
  private val ds = new TestEthSource(spark, inputDir)
  private val bucketSize = 2
  private val prefixLength = 4
  private val t = new EthTransformation(spark, bucketSize)

  // read raw data
  val blocks = ds.blocks()
  val transactions = ds.transactions()
  val traces = ds.traces()
  val exchangeRatesRaw = ds.exchangeRates()

  val tokenTransfers = spark.emptyDataset[TokenTransfer]
  val encodedTokenTransfers = spark.emptyDataset[EncodedTokenTransfer]
  val contracts = spark.emptyDataset[Contract]

  // read ref values
  val transactionIdsRef =
    readTestData[TransactionId](refDir + "transactions_ids.csv")

  val transactionIdsGroupRef =
    readTestData[TransactionIdByTransactionIdGroup](
      refDir + "transactions_ids_by_id_group.csv"
    )

  val transactionIdsPrefixRef =
    readTestData[TransactionIdByTransactionPrefix](
      refDir + "transactions_ids_by_prefix.csv"
    )

  val addressIdsRef =
    readTestData[AddressId](refDir + "address_ids.csv")

  val addressIdsPrefixRef = readTestData[AddressIdByAddressPrefix](
    refDir + "address_ids_by_prefix.csv"
  )

  val exchangeRatesRef =
    readTestData[ExchangeRates](refDir + "exchange_rates.json")

  val encodedTransactionsRef =
    readTestData[EncodedTransaction](
      refDir + "encoded_transactions.json"
    )

  val addressTransactionsRef =
    readTestData[AddressTransaction](
      refDir + "address_transactions.json"
    )

  val blockTransactionsRef =
    readTestData[BlockTransaction](refDir + "block_transactions.json")

  val addressesRef =
    readTestData[Address](refDir + "addresses.json")

  val addressRelationsRef =
    readTestData[AddressRelation](refDir + "address_relations.json")

  // Compute values
  val noBlocks = blocks.count.toInt
  val lastBlockTimestamp = blocks
    .select(max(col("timestamp")))
    .first
    .getInt(0)
  val noTransactions = transactions.count()

  val exchangeRates =
    t.computeExchangeRates(blocks, exchangeRatesRaw)
      .persist()

  val transactionIds = t.computeTransactionIds(transactions)
  val transactionIdsByTransactionIdGroup =
    transactionIds.toDF.transform(
      TransformHelpers.withSortedIdGroup[TransactionIdByTransactionIdGroup](
        "transactionId",
        "transactionIdGroup",
        bucketSize
      )
    )
  val transactionIdsByTransactionPrefix =
    transactionIds.toDF.transform(
      TransformHelpers.withSortedPrefix[TransactionIdByTransactionPrefix](
        "transaction",
        "transactionPrefix",
        prefixLength
      )
    )
  val addressIds = t
    .computeAddressIds(traces, tokenTransfers)
    .sort("addressId")

  val addressIdsByAddressPrefix =
    addressIds.toDF
      .transform(
        TransformHelpers.withSortedPrefix[AddressIdByAddressPrefix](
          "address",
          "addressPrefix",
          prefixLength
        )
      )
      .sort("addressId")

  val encodedTransactions =
    t.computeEncodedTransactions(
      traces,
      transactionIds,
      addressIds,
      exchangeRates
    ).sort("blockId")
      .persist()

  val blockTransactions = t
    .computeBlockTransactions(blocks, encodedTransactions)
    .sort("blockId")

  val addressTransactions = t
    .computeAddressTransactions(
      encodedTransactions,
      encodedTokenTransfers
    )
    .persist()

  val addresses = t
    .computeAddresses(
      encodedTransactions,
      encodedTokenTransfers,
      addressTransactions,
      addressIds,
      contracts
    )
    .persist()

  val addressRelations = t
    .computeAddressRelations(encodedTransactions, encodedTokenTransfers)
    .sort("srcAddressId", "dstAddressId")

  note("Test lookup tables")

  test("Transaction IDs") {
    assertDataFrameEquality(transactionIds, transactionIdsRef)
  }
  test("Transaction IDs by ID group") {
    assertDataFrameEquality(
      transactionIdsByTransactionIdGroup,
      transactionIdsGroupRef
    )
  }
  test("Transaction IDs by transaction prefix") {
    assertDataFrameEquality(
      transactionIdsByTransactionPrefix,
      transactionIdsPrefixRef
    )
  }

  test("no duplicates in block txs") {
    assert(
      blockTransactions
        .filter(size(col("txs")) =!= size(array_distinct(col("txs"))))
        .count() === 0
    )
  }

  test("Address IDs") {
    assertDataFrameEquality(addressIds, addressIdsRef)
  }
  test("Address IDs by address prefix") {
    assertDataFrameEquality(addressIdsByAddressPrefix, addressIdsPrefixRef)
  }

  note("Test exchange rates")

  test("Exchange rates") {
    assertDataFrameEquality(exchangeRates, exchangeRatesRef)
  }

  test("Encoded transactions") {
    assertDataFrameEquality(
      encodedTransactions.filter($"transactionId".isNotNull),
      encodedTransactionsRef
    )
  }

  note("Test blocks")

  test("Block transactions") {
    assertDataFrameEquality(blockTransactions, blockTransactionsRef)
  }

  note("Test address graph")

  test("Address transactions") {
    assertDataFrameEquality(addressTransactions, addressTransactionsRef)
  }

  test("Addresses") {
    assertDataFrameEqualityGeneric(
      addresses,
      addressesRef,
      ignoreCols = List(
        "noIncomingTxsCode",
        "noOutgoingTxsCode",
        "inDegreeCode",
        "outDegreeCode"
      )
    )
  }

  test("Address relations") {
    assertDataFrameEquality(addressRelations, addressRelationsRef)
  }

  test("Check statistics") {
    assert(blocks.count.toInt == 84, "expected 84 blocks")
    assert(lastBlockTimestamp == 1438919571)
    assert(transactions.count() == 10, "expected 10 transaction")
    assert(addressIds.count() == 59, "expected 59 addresses")
    assert(addressRelations.count() == 9, "expected 9 address relations")
  }
}
