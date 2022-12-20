package info.graphsense

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.functions.{col, lit, udf, forall}
import org.scalatest.funsuite.AnyFunSuite

import Helpers.{readTestData}
import info.graphsense.contract.tokens.Erc20
import info.graphsense.Conversion._
import org.apache.spark.sql.{Dataset, Column}

import Helpers.{readTestData, setNullableStateForAllColumns}

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
    val b = hexstr_to_bytes(original)
    assert(bytes_to_hexstr(b) == original)
  }

  test("test convertions 2") {
    val original =
      "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
        .toLowerCase()
    assert(bytes_to_hexstr(hexstr_to_bytes(original)) == original)
  }

  test("Default Transfer") {
    val dv = TokenTransfer.default()

    assert(dv.isDefault)
  }

  test("decode log") {

    /*    {
      "block_id_group": 15000,
      "block_id": 15000000,
      "topic0": "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
      "log_index": 0,
      "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
      "block_hash": "0x9a71a95be3fe957457b11817587e5af4c7e24836d5b383c430ff25b9286a457f",
      "data": "0x0000000000000000000000000000000000000000000000005ea0a1fe55143c0d",
      "topics": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", "0x00000000000000000000000006729eb2424da47898f935267bd4a62940de5105", "0x000000000000000000000000beefbabeea323f07c59926295205d3b7a17e8638"],
      "transaction_index": 2,
      "tx_hash": "0xbeb3d09a644dc0772719f498172af05ed8cd337aaf83c2b5aee43be34fcb9dfb"
    }
     */
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

    val t = Erc20.decode_transfer(l)

    assert(t.get === e)
    assert(t.get.isDefault == false)

  }

  test("full transform with logs") {

    val inputDir = "src/test/resources/tokens/"
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val logs = readTestData[Log](spark, inputDir + "logs.json")

    /*val token_addresses = .map(x => lit(x))*/

    val tt = new TokenTransfers(spark)

    /* Parse all but only keep USDT token transfers for compare but parse all of them*/
    val transfers = tt
      .get_token_transfers(logs, tt.token_addresses)
      .filter(
        col("token_address") === lit(
          hexstr_to_bytes("0xdAC17F958D2ee523a2206206994597C13D831ec7")
        )
      )

    /* GENERATE REV DATA DECODED TOKEN TRANSFERS

    val transfers_str = tt.human_readable_token_transfers(transfers)
    println(transfers_str.show())

    transfers_str.write
      .format("csv")
      .option("header", true)
      .save(
        "/home/mf/Documents/ikna/src/infrastructure/graphsense-ethereum-transformation/test.csv"
      )
     */
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

    /* Parse all but only keep USDT token transfers for compare but parse all of them*/
    val transfers = tt
      .get_token_transfers(logs, tt.token_addresses)

    val token_configs = tt.get_token_configurations()

    val exchangeRates =
      t.computeExchangeRates(blocks, exchangeRatesRaw)
        .persist()

    val transactionIds = t.computeTransactionIds(transactions)

    val addressIds = t
      .computeAddressIds(traces, transfers)
      .sort("addressId")

    val encodedTransactions =
      t.computeEncodedTransactions(
        transactions,
        transactionIds,
        addressIds,
        exchangeRates
      )

    assert(
      encodedTransactions
        .withColumn(
          "allfiatset",
          forall(col("fiatValues"), (colmn: Column) => colmn.isNotNull)
        ).filter($"allfiatset"===lit(false)).count() === 0
    )
    /* GENERATE LIST OF HASHES used in Transfers   

    val htostr = udf((x: Array[Byte]) => bytes_to_hexstr(x))
    val hashes = transfers.select($"txHash").distinct().withColumn("txHash", htostr(transfers("txHash")))

    hashes.write
      .format("csv")
      .option("header", true)
      .save(
        "/home/mf/Documents/ikna/src/infrastructure/graphsense-ethereum-transformation/hashes.csv"
      )
     */
    val encodedTokenTransfers =
      t.computeEncodedTokenTransfers(
        transfers,
        token_configs,
        transactionIds,
        addressIds,
        exchangeRates
      ).persist()

    assert(
      encodedTokenTransfers
        .withColumn(
          "allfiatset",
          forall(col("fiatValues"), (colmn: Column) => colmn.isNotNull)
        ).filter($"allfiatset"===lit(false)).count() === 0
    )

    val addressTransactions = t
      .computeAddressTransactions(
        encodedTransactions,
        encodedTokenTransfers
      )



    val addresses = t
      .computeAddresses(
        encodedTransactions,
        encodedTokenTransfers,
        addressTransactions,
        addressIds
      )

    assert(
      addresses.count() === addresses.select($"addressId").distinct().count()
    )

    val addressesRef =
      readTestData[Address](
        spark,
        inputDir + "/reference/addresses.json"
      )

    assertDataFrameEquality(addresses, addressesRef)

/*    val htostr = udf((x: Array[Byte]) => bytes_to_hexstr(x))
    val addresseshr = addresses
      .withColumn("address", htostr(addresses("address")))
    addresseshr.write
      .format("json")
      .save(
        "/home/mf/Documents/ikna/src/infrastructure/graphsense-ethereum-transformation/addresses.json"
      )*/

  }

}
