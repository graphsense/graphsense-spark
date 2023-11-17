package org.graphsense

import org.graphsense.utxo.{TransformationJob => Utxo}
import org.graphsense.utxo.{TransformationJob => Account}
import org.rogach.scallop._

class CliArgs(arguments: Seq[String]) extends ScallopConf(arguments) {
  val network: ScallopOption[String] = opt[String](
    "network",
    required = true,
    noshort = true,
    descr =
      "Select which network we are processing (btc, zec, bch, ltc, eth, trx)"
  )
  verify()
}

object TransformationJob {

  def main(args: Array[String]) {

    val conf = new CliArgs(args)

    conf.network().toLowerCase() match {
      case "eth" | "trx"                 => Account.main(args)
      case "btc" | "zec" | "bch" | "ltc" => Utxo.main(args)
      case _ =>
        throw new IllegalArgumentException(
          "Network " + conf.network() + " not supported."
        )
    }

  }

}
