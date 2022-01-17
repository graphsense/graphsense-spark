package info.graphsense

// lookup tables

case class TransactionId(
    transaction: Array[Byte],
    transactionId: Int
)

case class TransactionIdByTransactionPrefix(
    transactionPrefix: String,
    transaction: Array[Byte],
    transactionId: Int
)

case class TransactionIdByTransactionIdGroup(
    transactionIdGroup: Int,
    transactionId: Int,
    transaction: Array[Byte]
)

case class AddressId(
    address: Array[Byte],
    addressId: Int
)

case class AddressIdByAddressPrefix(
    addressPrefix: String,
    address: Array[Byte],
    addressId: Int
)

// transformed schema data types

case class Currency(value: BigInt, fiatValues: Seq[Float])
case class AddressSummary(totalReceived: Currency, totalSpent: Currency)

// raw schema tables

case class Block(
    blockIdGroup: Int,
    blockId: Int,
    blockHash: Array[Byte],
    parentHash: Array[Byte],
    nonce: Array[Byte],
    sha3_uncles: Array[Byte],
    logsBloom: Array[Byte],
    transactionsRoot: Array[Byte],
    stateRoot: Array[Byte],
    receiptsRoot: Array[Byte],
    miner: Array[Byte],
    difficulty: BigInt,
    totalDifficulty: BigInt,
    size: Int,
    extraData: Array[Byte],
    gasLimit: Int,
    gasUsed: Int,
    baseFeePerGas: Option[Long],
    timestamp: Int,
    transactionCount: Short
)

case class Transaction(
    txHashPrefix: String,
    txHash: Array[Byte],
    nonce: Int,
    blockHash: Array[Byte],
    blockId: Int,
    transactionIndex: Short,
    fromAddress: Array[Byte],
    toAddress: Option[Array[Byte]],
    value: BigInt, // in wei, 1e18 wei = 1 ether
    gas: Int,
    gasPrice: BigInt,
    input: Array[Byte],
    blockTimestamp: Int,
    receiptGasUsed: BigInt
)

case class Trace(
    blockIdGroup: Int,
    blockId: Int,
    traceId: String,
    fromAddress: Option[Array[Byte]],
    toAddress: Option[Array[Byte]],
    value: BigInt,
    status: Int,
    callType: Option[String]
)

case class ExchangeRatesRaw(
    date: String,
    fiatValues: Option[Map[String, Float]]
)

// transformed schema tables

case class ExchangeRates(blockId: Int, fiatValues: Seq[Float])

case class Balance(addressIdGroup: Int, addressId: Int, balance: BigInt)

case class BlockTransaction(blockIdGroup: Int, blockId: Int, txs: Seq[Int])

case class EncodedTransaction(
    transactionId: Int,
    nonce: Int,
    blockId: Int,
    transactionIndex: Short,
    srcAddressId: Int,
    dstAddressId: Option[Int],
    value: BigInt,
    fiatValues: Seq[Float],
    gas: Int,
    gasPrice: BigInt,
    input: Array[Byte],
    blockTimestamp: Int
)

case class AddressTransaction(
    addressIdGroup: Int,
    addressIdSecondaryGroup: Int,
    addressId: Int,
    transactionId: Int,
    blockId: Int,
    blockTimestamp: Int,
    isOutgoing: Boolean
)

case class AddressTransactionSecondaryIds(
    addressIdGroup: Int,
    maxSecondaryId: Int
)

case class Address(
    addressIdGroup: Int,
    addressId: Int,
    address: Array[Byte],
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTxId: Int,
    lastTxId: Int,
    totalReceived: Currency,
    totalSpent: Currency,
    inDegree: Int,
    outDegree: Int
)

case class AddressRelation(
    srcAddressIdGroup: Int,
    srcAddressIdSecondaryGroup: Int,
    srcAddressId: Int,
    dstAddressIdGroup: Int,
    dstAddressIdSecondaryGroup: Int,
    dstAddressId: Int,
    noTransactions: Int,
    value: Currency
)

case class AddressOutgoingRelationSecondaryIds(
    srcAddressIdGroup: Int,
    maxSecondaryId: Int
)

case class AddressIncomingRelationSecondaryIds(
    dstAddressIdGroup: Int,
    maxSecondaryId: Int
)

case class SummaryStatistics(
    timestamp: Int,
    noBlocks: Long,
    noTransactions: Long,
    noAddresses: Long,
    noAddressRelations: Long
)

case class Configuration(
    keyspaceName: String,
    bucketSize: Int,
    txPrefixLength: Int,
    addressPrefixLength: Int,
    fiatCurrencies: Seq[String]
)
