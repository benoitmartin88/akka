/*
 * Copyright (C) 2021-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.actor.{ActorRef, ActorSystem}
import akka.cluster.ddata.Replicator._
import akka.event.{Logging, LoggingAdapter}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object Transaction {
  private[akka] type TransactionId = String

  final case class Context(replicator: ActorRef, actor: ActorRef, var version: VersionVector = VersionVector.empty)
      extends Serializable {
    val tid: TransactionId = java.util.UUID.randomUUID.toString // TODO: 128 bits can be reduced. eg: Snowflake ?

    def get[T <: ReplicatedData](key: Key[T], request: Option[Any] = None): Unit = {
      replicator.tell(Get(key, ReadLocal, request, Option(this)), actor)
    }

//    def update[T <: ReplicatedData](key: Key[T], initial: T)(modify: T => T): Unit = {
//      replicator ! Update(key, initial, WriteLocal, None, Option(tid))(modify)
//    }

//    def update[T <: ReplicatedData](key: Key[T])(modify: Option[T] => T): Unit = {
//      replicator ! Update(key, WriteLocal, None, Option(tid))(modify)
//    }

    def update[T <: ReplicatedData](key: Key[T], request: Option[Any] = None)(value: T): Unit = {
      replicator.tell(Update(key, WriteLocal, request, Some(tid))(_ => value), actor)
    }
  }
}

/**
 * TODO:
 * - remove replicator from ctor arguments
 * - auto commit at the end of the transaction scope
 */
final case class Transaction(system: ActorSystem, actor: ActorRef, var operations: (Transaction.Context) => Unit) {
  import akka.cluster.ddata.Transaction.{Context, TransactionId}

//  def apply(actorContext: ActorContext, operations: (Transaction.Context) => Unit): Transaction = {
////    val system = actorContext.system
//
//    val replicator = DistributedData(actorContext.system).replicator
////    val replicator = system.actorOf(
////      Replicator.props(ReplicatorSettings(system).withGossipInterval(1.second).withMaxDeltaElements(10)),
////      "replicator")
//    Transaction(replicator, actorContext.self, operations)
//  }

  val replicator = DistributedData(system).replicator
  val context: Context = Context(replicator, actor)
  val id: TransactionId = context.tid
  val log: LoggingAdapter = Logging(system, Transaction.getClass)
  private implicit val askTimeout: Timeout = 5.seconds

  prepare()

  private def prepare(): Boolean = {
    if(log.isDebugEnabled) log.debug("[{}] - prepare()", id)
    assert(context.version.isEmpty)

    try {
      Await.result((replicator ? TwoPhaseCommitPrepare(id)).mapTo[TwoPhaseCommitPrepareResponse], askTimeout.duration) match {
        case TwoPhaseCommitPrepareSuccess(v, _) =>
          context.version = v
          true
        case TwoPhaseCommitPrepareError(msg, _) =>
          log.error(msg)
          throw new RuntimeException(msg)
        case _ =>
          log.error("Unexpected message")
          throw new RuntimeException("Unexpected message")
      }
    } catch {
      case e: Throwable =>
        log.error("[{}] - Transaction::prepare() [{}]", id, e.getMessage)
        abort()
        throw e
    }
  }

  /**
   * Blocking call because of 2-phase-commit.
   * @return
   */
  def commit(): Boolean = {
    if(log.isDebugEnabled) log.debug("[{}] - commit() [{}]", id, context.version)

    try {
      operations(context)

      Await.result(
        (replicator ? TwoPhaseCommitCommit(context)).mapTo[TwoPhaseCommitCommitResponse],
        askTimeout.duration) match {
        case TwoPhaseCommitCommitSuccess(_) => true
        case TwoPhaseCommitCommitError(msg, _) =>
          log.error(msg)
          false
        case _ =>
          log.error("Unexpected message")
          false
      }
    } catch {
      case e: Throwable =>
        log.error(e.getMessage)
        abort()
        throw e
    }
  }

  def abort(): Unit = {
    replicator ! TwoPhaseCommitAbort(id) // TODO: wait for ACK ?
  }

}
