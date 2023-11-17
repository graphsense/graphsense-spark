package org.graphsense.account.eth.tokens

import org.graphsense.account.Implicits._
import org.graphsense.account.TokenSet
import org.graphsense.account.models.TokenConfiguration

import math.pow

object EthTokenSet extends TokenSet {

  private val supportedTokens = List(
    TokenConfiguration(
      "USDT",
      hexStrToBytes("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
      "erc20",
      6,
      pow(10, 6).longValue(),
      Some("USD")
    ),
    TokenConfiguration(
      "USDC",
      hexStrToBytes("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
      "erc20",
      6,
      pow(10, 6).longValue(),
      Some("USD")
    ),
    TokenConfiguration(
      "WETH",
      hexStrToBytes("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"),
      "erc20",
      18,
      pow(10, 18).longValue(),
      Some("ETH")
    )
  )

  def getSupportedTokens(): List[TokenConfiguration] = {
    supportedTokens
  }

}
