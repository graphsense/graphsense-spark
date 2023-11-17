package org.graphsense.models

// generic transform datatypes
case class ExchangeRatesRaw(
    date: String,
    fiatValues: Option[Map[String, Float]]
)

case class ExchangeRates(blockId: Int, fiatValues: Seq[Float])
