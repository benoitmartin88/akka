/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.actor.{Actor, ActorLogging, ActorRef, CausalMessageWrapper, Metadata}
import akka.cluster.ddata.Replicator.{CausalChange, SubscribeToCausalChange}

import scala.annotation.unused
import scala.collection.mutable

class CausalActor extends Actor with ActorLogging {
  val replicator: ActorRef = DistributedData(context.system).replicator
  var causalContext: Metadata = Metadata(VersionVector.empty)
  var buffer: mutable.Queue[CausalMessageWrapper] = mutable.Queue.empty

  replicator ! SubscribeToCausalChange(self)

  /**
   * Scala API: This defines the initial actor behavior, it must return a partial function
   * with the actor logic.
   */
  def receive: Receive = {
    case msg: CausalChange => // TODO: check that this is received from replicator
      println("!!!!! CausalActor::receive() -> CausalChange msg= " + msg)
      causalContext.gss = msg.versionVector
      checkAndDeliverCausalMessages() // check if buffered messages can be delivered.
      onCausalChange(causalContext)
    case msg: CausalMessageWrapper =>
      if (checkIfCausallyDeliverable(msg)) {
        println("!!!!! CausalActor::receive() -> DELIVER")
        self.forward(msg.message)
        checkAndDeliverCausalMessages() // check if buffered messages can be delivered.
      } else {
        // buffer message and wait for causal context to be correct
        println("!!!!! CausalActor::receive() -> ENQUEUE")
        buffer.enqueue(msg)
      }
  }

  private def checkIfCausallyDeliverable(msg: CausalMessageWrapper): Boolean = {
    println(">>>>>>>>>>>> checkIfCausallyDeliverable() msg.metadata= " + msg.metadata.lastDeliveredSequenceMatrix)
    assert(msg.to == self)
    assert(causalContext.gss != None)
    assert(causalContext.gss.isInstanceOf[VersionVector])
    assert(msg.metadata.gss != None)
    assert(msg.metadata.gss.isInstanceOf[VersionVector])

    val lastSeenSeqNumber =
      causalContext.lastDeliveredSequenceMatrix.getOrElse(self, Map.empty[ActorRef, Long]).getOrElse(msg.from, 0L)
    val msgSeqNumber = msg.metadata.lastDeliveredSequenceMatrix(self)(msg.from)
    val dependencies = msg.metadata.lastDeliveredSequenceMatrix(self).filter(x => x._1 != msg.from)
    val lastSeenGss = causalContext.gss.asInstanceOf[VersionVector]
    val msgGss = msg.metadata.gss.asInstanceOf[VersionVector]

    println(
      ">>>>>>>>>>>> checkIfCausallyDeliverable() lastSeenSeqNumber=" + lastSeenSeqNumber +
      ", msgSeqNumber=" + msgSeqNumber +
      ", msgGss=" + msgGss +
      ", lastSeenGss=" + lastSeenGss +
      ", dependencies=" + dependencies)

    if ((msgGss == lastSeenGss || msgGss < lastSeenGss) && // wait for shared memory
        lastSeenSeqNumber == msgSeqNumber - 1 && // consecutive sequence number
        dependencies.forall( // check if dependencies are satisfied
          x =>
            x._2 == causalContext.lastDeliveredSequenceMatrix
                .getOrElse(self, Map.empty[ActorRef, Long])
                .getOrElse(x._1, 0L))) {
      updateLocalCausalContext(msg)
      true
    } else {
      false
    }
  }

  private def updateLocalCausalContext(msg: CausalMessageWrapper): Unit = {
    // update causalContext
    causalContext.gss =
      causalContext.gss.asInstanceOf[VersionVector].merge(msg.metadata.gss.asInstanceOf[VersionVector])
    causalContext.lastDeliveredSequenceMatrix ++= msg.metadata.lastDeliveredSequenceMatrix
  }

  // TODO: find a more efficient way of doing this. No need to iterate through all messages
  private def checkAndDeliverCausalMessages(): Unit = {
    buffer.foreach(msg => {
      if (checkIfCausallyDeliverable(msg)) {
        println("!!!!! CausalActor::receive() -> DELIVER")
        self.forward(msg.message)
        buffer = buffer.filter(mm => mm == msg)
      }
    })
  }

  def onCausalChange(@unused causalContext: Metadata): Unit = {}

}
