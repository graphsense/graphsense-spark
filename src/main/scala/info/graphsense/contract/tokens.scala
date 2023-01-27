package info.graphsense.contract.tokens

import info.graphsense.{TokenTransfer, Log}
import info.graphsense.Conversion._

import org.web3j.abi.datatypes.Event;
import org.web3j.abi.TypeReference;
import org.web3j.abi.FunctionReturnDecoder;

import scala.collection.JavaConverters._
import org.web3j.abi.EventEncoder
import java.math.BigInteger
import scala.util.Try
import scala.util.Success
import scala.util.Failure

object Erc20 {

  val tokenTransferEvent: Event = new Event(
    "Transfer",
    List(
      TypeReference.makeTypeReference("address", true, false),
      TypeReference.makeTypeReference("address", true, false),
      TypeReference.makeTypeReference("uint256", false, false)
    ).asInstanceOf[List[TypeReference[_]]].asJava
  );

  val tokenTransferEventSelector = EventEncoder.encode(tokenTransferEvent)

  val transfer_topic_hash = hexstr_to_bytes(
    EventEncoder.encode(tokenTransferEvent)
  )

  def decode_transfer(log: Log): Try[TokenTransfer] = {
    val topic0_str = bytes_to_hexstr(log.topic0)
    try {
      topic0_str match {
        case `tokenTransferEventSelector` => {
          val iparam = tokenTransferEvent.getIndexedParameters()
          val dparam = tokenTransferEvent.getNonIndexedParameters()
          val sender = FunctionReturnDecoder
            .decodeIndexedValue(
              bytes_to_hexstr_can(log.topics(1)),
              iparam.get(0)
            )
            .getValue()
            .asInstanceOf[String]
          val recipient = FunctionReturnDecoder
            .decodeIndexedValue(
              bytes_to_hexstr_can(log.topics(2)),
              iparam.get(1)
            )
            .getValue()
            .asInstanceOf[String]
          val value = BigInt(
            FunctionReturnDecoder
              .decode(bytes_to_hexstr_can(log.data), dparam)
              .get(0)
              .getValue()
              .asInstanceOf[BigInteger]
          )
          Success(
            TokenTransfer(
              log.blockId,
              log.transactionIndex,
              log.logIndex,
              log.txHash,
              log.address,
              sender,
              recipient,
              value
            )
          )
        }
        case _ => { Failure(new Exception("Wrong topic0, can't decode")) }
      }
    } catch {
      case e: Throwable => Failure(e)
    }
  }

}
