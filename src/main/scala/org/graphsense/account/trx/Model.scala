package org.graphsense.account.trx.models

case class Trace(
    blockIdGroup: Int,
    blockId: Int,
    internalIndex: Int,
    traceIndex: Int,
    callerAddress: Option[Array[Byte]],
    transfertoAddress: Option[Array[Byte]],
    callInfoIndex: Int,
    callTokenId: Int,
    callValue: Int,
    note: String,
    rejected: Boolean
)

case class Trc10(
    id: Int,
    ownerAddress: Array[Byte],
    name: String,
    abbr: String,
    description: String,
    totalSupply: BigInt,
    url: String,
    precision: Int
)

// case class TrcFrozenSupply(
//     frozenAnmount: BigInt,
//     frozenDays:BigInt
// )
