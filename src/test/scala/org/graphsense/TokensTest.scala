package org.graphsense

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions.{col, forall, lit}
import org.scalatest.funsuite.AnyFunSuite

import Helpers.{readTestData, setNullableStateForAllColumns}
import org.graphsense.Conversion._
import org.graphsense.contract.tokens.Erc20

class TokenTest
    extends AnyFunSuite
    with SparkSessionTestWrapper
    with DataFrameComparer {

  def assertDataFrameEquality[T](
      actualDS: Dataset[T],
      expectedDS: Dataset[T]
  ): Unit = {
    val colOrder: Array[Column] = expectedDS.columns.map(col)

    assertSmallDataFrameEquality(
      setNullableStateForAllColumns(actualDS.select(colOrder: _*)),
      setNullableStateForAllColumns(expectedDS)
    )
  }

  test("test convertions") {
    val original = "0xdAC17F958D2ee523a2206206994597C13D831ec7".toLowerCase()
    val b = hexStrToBytes(original)
    assert(bytesToHexStr(b) == original)
  }

  test("test convertions 2") {
    val original =
      "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        .toLowerCase()
    assert(bytesToHexStr(hexStrToBytes(original)) == original)
  }

  test("Default Transfer") {
    assert(TokenTransfer.default().isDefault)
  }

  test("decode log") {
    val l = Log(
      15000,
      15000000,
      "0x9a71a95be3fe957457b11817587e5af4c7e24836d5b383c430ff25b9286a457f",
      "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
      "0x0000000000000000000000000000000000000000000000005ea0a1fe55143c0d",
      Seq(
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
        "0x00000000000000000000000006729eb2424da47898f935267bd4a62940de5105",
        "0x000000000000000000000000beefbabeea323f07c59926295205d3b7a17e8638"
      ),
      "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
      "0xbeb3d09a644dc0772719f498172af05ed8cd337aaf83c2b5aee43be34fcb9dfb",
      0,
      2
    )

    val e = TokenTransfer(
      15000000,
      2,
      0,
      "0xbeb3d09a644dc0772719f498172af05ed8cd337aaf83c2b5aee43be34fcb9dfb",
      "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
      "0x06729eb2424da47898f935267bd4a62940de5105",
      "0xbeefbabeea323f07c59926295205d3b7a17e8638",
      "5ea0a1fe55143c0d"
    )

    val t = Erc20.decodeTransfer(l)

    assert(t.get === e)
    assert(t.get.isDefault == false)

  }

test("full transform with logs") {

    val inputDir = "src/test/resources/tokens/"
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val logs = readTestData[Log](spark, inputDir + "logs.json")

    val tt = new TokenTransfers(spark)

    // Parse all but only keep USDT token transfers for compare but parse all of them
    val transfers = tt
      .getTokenTransfers(logs, tt.tokenAddresses)
      .filter(
        col("tokenAddress") === lit(
          hexStrToBytes("0xdAC17F958D2ee523a2206206994597C13D831ec7")
        )
      )

    val transfersRef =
      readTestData[TokenTransfer](
        spark,
        inputDir + "/reference/USDT_token_transfers.csv"
      )

    assertDataFrameEquality(transfers, transfersRef)

  }
  
 test("encoded token transfers test") {
    import spark.implicits._
    val inputDir = "src/test/resources/tokens/"

    val blocks =
      readTestData[Block](spark, inputDir + "test_blocks.csv")

    val transactions =
      readTestData[Transaction](
        spark,
        inputDir + "test_transactions.csv"
      )

    val traces =
      readTestData[Trace](spark, inputDir + "test_traces.csv")
    val exchangeRatesRaw =
      readTestData[ExchangeRatesRaw](
        spark,
        inputDir + "test_exchange_rates.json"
      )
    val logs = readTestData[Log](spark, inputDir + "logs.json")

    val t = new Transformation(spark, 2)
    val tt = new TokenTransfers(spark)

    // parse all but only keep USDT token transfers for compare but parse all of them
    val transfers = tt
      .getTokenTransfers(logs, tt.tokenAddresses)

    val tokenConfigs = tt.getTokenConfigurations()

    val exchangeRates =
      t.computeExchangeRates(blocks, exchangeRatesRaw)
        .persist()

    val transactionIds = t.computeTransactionIds(transactions)

    val addressIds = t
      .computeAddressIds(traces, transfers)
      .sort("addressId")

    val contracts = t.computeContracts(traces, addressIds)

    val encodedTransactions =
      t.computeEncodedTransactions(
        traces,
        transactionIds,
        addressIds,
        exchangeRates
      )

    assert(
      encodedTransactions
        .withColumn(
          "allfiatset",
          forall(col("fiatValues"), (colmn: Column) => colmn.isNotNull)
        )
        .filter(col("allfiatset") === lit(false))
        .count() === 0
    )


    assert(
      encodedTransactions.filter(col("dstAddressId").isNull).count() === 0
    )

    assert(
      encodedTransactions.filter(col("dstAddressId").isNull).count() === 0
    )

    assert(
      encodedTransactions
        .filter(col("dstAddressId").isNull)
        .count() === transactions.filter(col("toAddress").isNull).count()
    )

    val encodedTokenTransfers =
      t.computeEncodedTokenTransfers(
        transfers,
        tokenConfigs,
        transactionIds,
        addressIds,
        exchangeRates
      ).persist()

    assert(
      encodedTokenTransfers
        .withColumn(
          "allfiatset",
          forall(col("fiatValues"), (colmn: Column) => colmn.isNotNull)
        )
        .filter(col("allfiatset") === lit(false))
        .count() === 0
    )

    assert(
      encodedTokenTransfers.filter(col("srcAddressId").isNull).count() === 0
    )

    assert(
      encodedTokenTransfers.filter(col("dstAddressId").isNull).count() === 0
    )

    
    val addressTransactions = t
      .computeAddressTransactions(
        encodedTransactions,
        encodedTokenTransfers
      )

    assert(
      addressTransactions
        .filter(col("transactionId").isNull)
        .count() === 0
    )

    val addresses = t
      .computeAddresses(
        encodedTransactions,
        encodedTokenTransfers,
        addressTransactions,
        addressIds,
        contracts
      )

    assert(
      addresses
        .filter(col("totalTokensSpent").isNotNull)
        .filter(col("totalSpent.value") > 0)
        .count() === 31
    )
    

    assert(
      addresses
        .filter(col("isContract") === true)
        .count() === 3
    )

    assert(
      addresses
        .filter(col("noIncomingTxs") === 0 && col("noOutgoingTxs") === 0)
        .count() === 0
    )

    
    // this is currently not true since for the address transactions, tx table is used not traces
    
    /*    val address_count = addressIds.count()
    assert(addresses.count() === address_count)*/
    

    assert(
      addresses.count() === addresses.select(col("addressId")).distinct().count()
    )

    assert(
      addresses.count() === addresses.select(col("address")).distinct().count()
    )
    
    val addressesRef =
      readTestData[Address](
        spark,
        inputDir + "/reference/addresses.json"
      )

    assertDataFrameEquality(addresses, addressesRef)
    
  
    val addressRelations =
      t.computeAddressRelations(encodedTransactions, encodedTokenTransfers)

    
    assert(
      addressRelations
        .filter(col("tokenValues").isNotNull)
        .filter(col("value.value") > 0)
        .count() === 1
    )
    assert(addressRelations.filter(col("srcAddressId").isNull).count() === 0)
    assert(addressRelations.filter(col("dstAddressId").isNull).count() === 0)
    assert(
      addressRelations
        .filter(col("tokenValues").isNotNull)
        .filter(col("value.value") > 0)
        .count() === 1
    )

    val addressRelationsRef =
      readTestData[AddressRelation](
        spark,
        inputDir + "/reference/address_relations.json"
      )

    assertDataFrameEquality(addressRelations, addressRelationsRef)

  }
}
