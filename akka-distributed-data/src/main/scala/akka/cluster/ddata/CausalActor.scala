/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.ddata.Replicator.WriteLocal

class CausalActor extends Actor with ActorLogging {
  println("<<<<<<<<<<<<<<<<<< CausalActor()")

  val replicator: ActorRef = DistributedData(context.system).replicator
//  val replicator = context.system.actorOf(
//    Replicator.props(ReplicatorSettings(context.system).withGossipInterval(1.second).withMaxDeltaElements(10)),
//    "replicator")
  val messageQueueKey: Key[MessageQueue] = MessageQueueKey.create(self)

  replicator ! Replicator.Subscribe(messageQueueKey, self)
  replicator ! Replicator.FlushChanges

  /**
   * Scala API: This defines the initial actor behavior, it must return a partial function
   * with the actor logic.
   */
  def receive: Receive = {
    case c @ Replicator.Changed(messageQueueKey) =>
      println("!!!!! CausalActor::receive() -> messageQueueKey=" + messageQueueKey + ", data=" + c.get(messageQueueKey))

      val q = c.get(messageQueueKey).asInstanceOf[MessageQueue].queue
      q.foreach(m => {
        println("----> m=" + m)
        self.tell(m._1, m._2)
      })

      // now empty queue
      replicator ! Replicator.Delete(messageQueueKey, WriteLocal, None)

//      assert(false)
    case Replicator.DeleteSuccess(_, None) =>
    case Replicator.Deleted(_) =>

//    case msg @ Replicator.Changed(key) =>
//      println("!!!!! CausalActor::receive() -> Changed msg= " + msg + ", key=" + key)
//
//      if(key.equals(messageQueueKey)) {
//        println(">>>>>>>>>>> YES !!!")
//      }

//      if(key.id == messageQueueKey.id) {
//        msg.dataValue.
//      }

//      msg.asInstanceOf[MessageQueue].queue.foreach(t => self.tell(t._1, t._2))
//    case other =>
//      println(">>>>>>>>>>>>>>>>> other=" + other)
//      assert(false)

  }

}
