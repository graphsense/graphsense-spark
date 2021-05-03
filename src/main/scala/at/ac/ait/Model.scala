package at.ac.ait

import java.sql.Date

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

case class AddressIdByAddressIdGroup(
    addressIdGroup: Int,
    addressId: Int,
    address: Array[Byte]
)

// transformed schema data types

case class Currency(value: BigInt, fiatValues: Seq[Float])
case class TxIdTime(height: Int, transactionId: Int, blockTimestamp: Int)
case class AddressSummary(totalReceived: Currency, totalSpent: Currency)

// raw schema tables

case class Block(
    blockGroup: Int,
    number: Int,
    hash: Array[Byte],
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
    timestamp: Int,
    transactionCount: Short
)

case class Transaction(
    hashPrefix: String,
    hash: Array[Byte],
    nonce: Int,
    blockHash: Array[Byte],
    blockNumber: Int,
    transactionIndex: Short,
    fromAddress: Array[Byte],
    toAddress: Option[Array[Byte]],
    value: BigInt, // in wei, 1e18 wei = 1 ether
    gas: Int,
    gasPrice: Long,
    input: Array[Byte],
    blockTimestamp: Int
)

case class ExchangeRatesRaw(
    date: String,
    fiatValues: Option[Map[String, Float]]
)

case class AddressTagRaw(
    address: String,
    currency: String,
    label: String,
    source: String,
    tagpackUri: String,
    lastmod: Date,
    category: Option[String],
    abuse: Option[String]
)

// transformed schema tables

case class ExchangeRates(height: Int, fiatValues: Seq[Float])

case class BlockTransaction(heightGroup: Int, height: Int, txs: Seq[Int])

case class EncodedTransaction(
    transactionId: Int,
    nonce: Int,
    height: Int,
    transactionIndex: Short,
    srcAddressId: Int,
    dstAddressId: Option[Int],
    value: BigInt,
    fiatValues: Seq[Float],
    gas: Int,
    gasPrice: Long,
    input: Array[Byte],
    blockTimestamp: Int
)

case class AddressTransaction(
    addressIdGroup: Int,
    addressIdSecondaryGroup: Int,
    addressId: Int,
    transactionId: Int,
    value: BigInt,
    height: Int,
    blockTimestamp: Int
)

case class AddressTransactionSecondaryIds(
    addressIdGroup: Int,
    maxSecondaryId: Int
)

case class Address(
    addressIdGroup: Int,
    addressId: Int,
    noIncomingTxs: Int,
    noOutgoingTxs: Int,
    firstTx: TxIdTime,
    lastTx: TxIdTime,
    totalReceived: Currency,
    totalSpent: Currency,
    inDegree: Int,
    outDegree: Int
)

case class AddressTag(
    addressId: Int,
    label: String,
    source: String,
    tagpackUri: String,
    lastmod: Int,
    category: String,
    abuse: String
)

case class AddressRelation(
    srcAddressIdGroup: Int,
    srcAddressIdSecondaryGroup: Int,
    srcAddressId: Int,
    dstAddressIdGroup: Int,
    dstAddressIdSecondaryGroup: Int,
    dstAddressId: Int,
    srcProperties: AddressSummary,
    dstProperties: AddressSummary,
    noTransactions: Int,
    value: Currency,
    transactionIdList: Seq[Int]
)

case class AddressOutgoingRelationSecondaryIds(
    srcAddressIdGroup: Int,
    maxSecondaryId: Int
)

case class AddressIncomingRelationSecondaryIds(
    dstAddressIdGroup: Int,
    maxSecondaryId: Int
)

case class Tag(
    labelNormPrefix: String,
    labelNorm: String,
    label: String,
    address: String,
    source: String,
    tagpackUri: String,
    currency: String,
    lastmod: Int,
    category: Option[String],
    abuse: Option[String],
    activeAddress: Boolean
)

case class SummaryStatistics(
    timestamp: Int,
    noBlocks: Long,
    noTransactions: Long,
    noAddresses: Long,
    noAddressRelations: Long,
    noTags: Long
)

case class Configuration(
    keyspaceName: String,
    bucketSize: Int,
    prefixLength: Int,
    fiatCurrencies: Seq[String]
)
