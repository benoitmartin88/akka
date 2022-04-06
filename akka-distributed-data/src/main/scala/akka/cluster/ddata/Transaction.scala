/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.actor.ActorRef
import akka.cluster.ddata.Replicator._
import akka.pattern.ask
import akka.util.Timeout
import org.slf4j.{ Logger, LoggerFactory }

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

object Transaction {
  private[akka] type TransactionId = String

  final case class Context(replicator: ActorRef, actor: ActorRef) {
    val tid: TransactionId = java.util.UUID.randomUUID.toString // TODO: 128 bits can be reduced. eg: Snowflake ?

    def get[T <: ReplicatedData](key: Key[T]): Unit = {
      replicator.tell(Get(key, ReadLocal, None, Option(tid)), actor)
    }

//    def update[T <: ReplicatedData](key: Key[T], initial: T)(modify: T => T): Unit = {
//      replicator ! Update(key, initial, WriteLocal, None, Option(tid))(modify)
//    }

//    def update[T <: ReplicatedData](key: Key[T])(modify: Option[T] => T): Unit = {
//      replicator ! Update(key, WriteLocal, None, Option(tid))(modify)
//    }

    def update[T <: ReplicatedData](key: Key[T])(value: T): Unit = {
      replicator.tell(Update(key, WriteLocal, None, Option(tid))(_ => value), actor)
    }
  }
}

/**
 * TODO:
 * - remove replicator from ctor arguments
 * - auto commit at the end of the transaction scope
 */
final case class Transaction(replicator: ActorRef, actor: ActorRef, operations: (Transaction.Context) => Unit) {
  import akka.cluster.ddata.Transaction.TransactionId
  import akka.cluster.ddata.Transaction.Context

  val context: Context = Context(replicator, actor)
  val id: TransactionId = context.tid
  val log: Logger = LoggerFactory.getLogger("akka.cluster.ddata.Transaction")
  private implicit val askTimeout: Timeout = 5.seconds

  /**
   * Blocking call because of 2-phase-commit.
   * @return
   */
  def commit(): Boolean = {
    log.debug("Transaction " + id + " commit()")

    try {
      prepare()
      operations(context)

      Await.result((replicator ? TwoPhaseCommitCommit(id)).mapTo[TwoPhaseCommitCommitResponse], askTimeout.duration) match {
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
        false
    }
  }

  def abort(): Unit = {
    replicator ! TwoPhaseCommitAbort(id) // TODO: wait for ACK ?
  }

  private def prepare(): Boolean = {
    log.debug("Transaction " + id + " prepare() " + replicator)
    // TODO
//    replicator ! TwoPhaseCommitPrepare(id) // TODO: wait for ACK ?

    try {
      Await.result((replicator ? TwoPhaseCommitPrepare(id)).mapTo[TwoPhaseCommitPrepareResponse], askTimeout.duration) match {
        case TwoPhaseCommitPrepareSuccess(_) => true
        case TwoPhaseCommitPrepareError(msg, _) =>
          log.error(msg)
          false
        case _ =>
          log.error("Unexpected message")
          false
      }
    } catch {
      case _: Throwable =>
        abort()
        false
    }
  }

}
