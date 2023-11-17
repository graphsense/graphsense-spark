package org.graphsense.account.trx.tokens

import org.graphsense.account.Implicits._
import org.graphsense.account.TokenSet
import org.graphsense.account.models.TokenConfiguration

import math.pow

object TrxTokenSet extends TokenSet {

  private val supportedTokens = List()

  def getSupportedTokens(): List[TokenConfiguration] = {
    supportedTokens
  }

}
