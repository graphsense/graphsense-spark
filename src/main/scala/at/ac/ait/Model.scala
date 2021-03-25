package at.ac.ait

case class AddressId(
    address: Array[Byte],
    addressId: Int
)

case class AddressIdByAddress(
    addressPrefix: String,
    address: Array[Byte],
    addressId: Int
)

case class AddressByIdGroup(
    addressIdGroup: Int,
    addressId: Int,
    address: Array[Byte]
)

case class TransactionId(
    transaction: Array[Byte],
    transactionId: Int
)

// transformed schema data types

case class TxSummary(
    txHash: Array[Byte],
    noInputs: Int,
    noOutputs: Int,
    totalInput: Long,
    totalOutput: Long
)

case class Currency(value: BigInt, usd: Float, eur: Float)

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
    transactionCount: Short,
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
    blockTimestamp: Int,
)

case class ExchangeRatesRaw(date: String, eur: Float, usd: Float)

case class TagRaw(
    address: String,
    label: String,
    source: String,
    tagpackUri: String,
    currency: String,
    lastmod: Int,
    category: Option[String],
    abuse: Option[String]
)

// transformed schema tables

case class ExchangeRates(height: Int, eur: Float, usd: Float)

case class BlockTransaction(height: Int, txs: Seq[Int])

case class EncodedTransaction(
    transactionId: Int,
    nonce: Int,
    height: Int,
    transactionIndex: Short,
    srcAddressId: Int,
    dstAddressId: Option[Int],
    value: Currency,
    gas: Int,
    gasPrice: Long,
    input: Array[Byte],
    blockTimestamp: Int
)


case class AddressTransaction(
    //addressIdGroup: Int, // TODO
    addressId: Int,
    transactionId: Int,
    value: BigInt,
    height: Int,
    blockTimestamp: Int
)

case class Address(
    //addressPrefix: String,
    //address: String,
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
    address: String,
    label: String,
    source: String,
    tagpackUri: String,
    lastmod: Int,
    category: String,
    abuse: String
)

case class AddressRelation(
    //srcAddressIdGroup: Int,
    srcAddressId: Int,
    //dstAddressIdGroup: Int,
    dstAddressId: Int,
    srcProperties: AddressSummary,
    dstProperties: AddressSummary,
    noTransactions: Int,
    value: Currency
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
    noBlocks: Int,
    noTransactions: Long,
    noAddresses: Long,
    noAddressRelations: Long,
    noTags: Long,
)

case class Configuration(
    keyspaceName: String,
    bucketSize: Int
)
