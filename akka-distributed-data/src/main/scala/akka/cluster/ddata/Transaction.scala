/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.actor.ActorRef
import akka.cluster.ddata.Replicator.TwoPhaseCommitPrepare

class Transaction(replicator: ActorRef ,operations: () => Unit) {

  val id = 0  // TODO

  /**
   * Blocking call because of 2-phase-commit.
   * @return
   */
  def commit(): Boolean = {

    try {
      prepare()
      operations()
      true
    } catch {
      case _: Throwable =>
        // rollback
//        t.printStackTrace()
        false
    }

  }

  def abort(): Boolean = {
    false
  }

  private def prepare(): Unit = {
    println("Transaction " + id + " prepare() " + replicator)
    // TODO
    replicator ! TwoPhaseCommitPrepare(id)
  }

}
