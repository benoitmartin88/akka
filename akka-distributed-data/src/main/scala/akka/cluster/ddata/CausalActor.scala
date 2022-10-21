/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.ddata.Replicator.{CausalChange, SubscribeToCausalChange}

import scala.annotation.unused
import scala.collection.mutable


class CausalActor extends Actor with ActorLogging {
  val replicator: ActorRef = DistributedData(context.system).replicator
  var lastSeenVersion: VersionVector = VersionVector.empty
  var buffer: mutable.Queue[CausalMessageWrapper] = mutable.Queue.empty

  replicator ! SubscribeToCausalChange(self)

  /**
   * Scala API: This defines the initial actor behavior, it must return a partial function
   * with the actor logic.
   */
  def receive: Receive = {
    case msg: CausalChange =>
      println("!!!!! CausalActor::receive() -> CausalChange msg= " + msg)
      lastSeenVersion = msg.versionVector
      checkAndDeliverCausalMessages() // check if buffered messages can be delivered.
      onCausalChange(lastSeenVersion)
    case msg: CausalMessageWrapper =>
      if (checkIfCausallyDeliverable(msg)) {
        println("!!!!! CausalActor::receive() -> DELIVER")
        msg.messages.foreach(m => self.forward(m))
        checkAndDeliverCausalMessages() // check if buffered messages can be delivered.
      } else {
        // buffer message and wait for causal context to be correct
        println("!!!!! CausalActor::receive() -> ENQUEUE")
        buffer.enqueue(msg)
      }
  }

  private def checkIfCausallyDeliverable(msg: CausalMessageWrapper): Boolean = {
    assert(msg.version != VersionVector.empty)

    val fromUniqueAddress = msg.fromNode
    val msgSeqNumber = msg.version.versionAt(fromUniqueAddress)
    val dependencies = msg.version.pruningCleanup(fromUniqueAddress) // TODO check this

    println(
      ">>>>>>>>>>>> checkIfCausallyDeliverable() lastSeenVersion=" + lastSeenVersion +
      ", msg.version=" + msg.version +
      ", fromUniqueAddress=" + fromUniqueAddress +
      ", msgSeqNumber=" + msgSeqNumber +
      ", check=" + (msg.version == lastSeenVersion || msg.version < lastSeenVersion) +
      ", dependencies=" + dependencies)

    if ((msg.version == lastSeenVersion || msg.version < lastSeenVersion) && // wait for shared memory
        lastSeenVersion.versionAt(fromUniqueAddress) >= msgSeqNumber && // consecutive sequence number
        dependencies.versionsIterator.forall(x => lastSeenVersion.versionAt(x._1) == x._2) // dependencies
        ) {
      lastSeenVersion = lastSeenVersion.merge(msg.version)
      true
    } else {
      false
    }
  }

  // TODO: find a more efficient way of doing this. No need to iterate through all messages
  private def checkAndDeliverCausalMessages(): Unit = {
    buffer.foreach(msg => {
      if (checkIfCausallyDeliverable(msg)) {
        println("!!!!! CausalActor::receive() -> DELIVER")
        msg.messages.foreach(m => self.forward(m))
        buffer = buffer.filter(mm => mm == msg)
      }
    })
  }

  def onCausalChange(@unused version: VersionVector): Unit = {}

}
