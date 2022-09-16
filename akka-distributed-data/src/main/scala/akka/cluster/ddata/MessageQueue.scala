/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.actor.ActorRef
import akka.util.HashCode

import scala.collection.mutable

object MessageQueue {
  type QueueValues = (Any, ActorRef)
  type Queue = mutable.Queue[QueueValues]

//  val empty: MessageQueue = new MessageQueue(mutable.Queue.empty)
  def empty(): MessageQueue = new MessageQueue(mutable.Queue.empty)
  def apply(): MessageQueue = empty()

  /**
   * Java API
   */
  def create(): MessageQueue = empty()
//  def unapply(c: PNCounter): Option[BigInt] = Some(c.value)
}

@SerialVersionUID(1L)
final class MessageQueue private[akka] (private[akka] val queue: MessageQueue.Queue)
    extends ReplicatedData
    with ReplicatedDataSerialization {

  override type T = MessageQueue

  /**
   * Scala API: Current total value of the counter.
   */
  def enqueue(msg: Any, from: ActorRef): MessageQueue = {
    queue.enqueue((msg, from))
    this
  }

  override def merge(that: MessageQueue): MessageQueue =
    copy(queue = this.queue ++ that.queue) // TODO: is this correct?

  private def copy(queue: MessageQueue.Queue): MessageQueue = new MessageQueue(queue)

  override def toString: String = s"MessageQueue($queue)"

  override def equals(o: Any): Boolean = o match {
    case other: MessageQueue => queue == other.queue
    case _                   => false
  }

  override def hashCode: Int = {
    var result = HashCode.SEED
    result = HashCode.hash(result, queue)
    result
  }
}

object MessageQueueKey {
  def create(to: ActorRef): Key[MessageQueue] = {
    MessageQueueKey(to.path.uid.toString) // TODO: check this
  } // TODO
}

@SerialVersionUID(1L)
final case class MessageQueueKey(_id: String) extends Key[MessageQueue](_id) with ReplicatedDataSerialization
