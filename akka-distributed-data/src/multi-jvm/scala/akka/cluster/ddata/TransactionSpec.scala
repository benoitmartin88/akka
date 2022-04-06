/*
 * Copyright (C) 2009-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.cluster.Cluster
import akka.cluster.ddata.Transaction.TransactionId
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.testkit._
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

object TransactionSpec extends MultiNodeConfig {
  val first = role("first")
  val second = role("second")
  val third = role("third")

  commonConfig(ConfigFactory.parseString("""
    akka.loglevel = DEBUG
    akka.actor.provider = "cluster"
    akka.log-dead-letters-during-shutdown = off
    #akka.cluster.distributed-data.delta-crdt.enabled = off
    """))

  testTransport(on = true)

}

class TransactionSpecMultiJvmNode1 extends TransactionSpec
class TransactionSpecMultiJvmNode2 extends TransactionSpec
class TransactionSpecMultiJvmNode3 extends TransactionSpec

class TransactionSpec extends MultiNodeSpec(TransactionSpec) with STMultiNodeSpec with ImplicitSender {
  import Replicator._
  import TransactionSpec._

  override def initialParticipants = roles.size

  val cluster = Cluster(system)
  implicit val selfUniqueAddress: SelfUniqueAddress = DistributedData(system).selfUniqueAddress
  val replicator = system.actorOf(
    Replicator.props(ReplicatorSettings(system).withGossipInterval(1.second).withMaxDeltaElements(10)),
    "replicator")
  val timeout = 3.seconds.dilated

//  val KeyA = GCounterKey("A")
//  val KeyB = GCounterKey("B")

  var afterCounter = 0
  def enterBarrierAfterTestStep(): Unit = {
    afterCounter += 1
    enterBarrier("after-" + afterCounter)
  }

//  private implicit val askTimeout: Timeout = 5.seconds

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster.join(node(to).address)
    }
    enterBarrier(from.name + "-joined")
  }

  "2PC prepare" must {
    "handle already existing transaction" in {
      val tid = "41"

      replicator ! TwoPhaseCommitPrepare(tid)
      expectMsg(TwoPhaseCommitPrepareSuccess(None))

      replicator ! TwoPhaseCommitPrepare(tid)
      expectMsg(TwoPhaseCommitPrepareError("Transaction id " + tid + " already inflight", None))
    }
  }

  "2PC abort" must {
    "succeed without prior prepare" in {
      replicator ! TwoPhaseCommitAbort("42")
      expectMsg(TwoPhaseCommitAbortSuccess(None))
    }

    "succeed with prior prepare while transaction is empty" in {
      val tid = "42"

      replicator ! TwoPhaseCommitPrepare(tid)
      expectMsg(TwoPhaseCommitPrepareSuccess(None))

      replicator ! TwoPhaseCommitAbort(tid)
      expectMsg(TwoPhaseCommitAbortSuccess(None))
    }

    "not modify data after abort" in {
      val tid = "42"
      val KeyB = GCounterKey("B")

      // prepare
      replicator ! TwoPhaseCommitPrepare(tid)
      expectMsg(TwoPhaseCommitPrepareSuccess(None))

      // KeyA should not be found
      replicator ! Get(KeyB, ReadLocal)
      expectMsg(NotFound(KeyB, None))

      // update key
      val c3 = GCounter() :+ 3

      replicator ! Update(KeyB, GCounter(), WriteLocal, None, Option(tid))(_ :+ 3)
      expectMsg(UpdateSuccess(KeyB, None))

      // get with transaction context
      replicator ! Get(KeyB, ReadLocal, None, Option(tid))
      expectMsg(GetSuccess(KeyB, None)(c3)).dataValue should be(c3)

      // get without transaction context
      replicator ! Get(KeyB, ReadLocal)
      expectMsg(NotFound(KeyB, None))

      // abort
      replicator ! TwoPhaseCommitAbort(tid)
      expectMsg(TwoPhaseCommitAbortSuccess(None))

      // get with transaction context
      replicator ! Get(KeyB, ReadLocal, None, Option(tid))
      expectMsg(NotFound(KeyB, None))

      // get without transaction context
      replicator ! Get(KeyB, ReadLocal)
      expectMsg(NotFound(KeyB, None))
    }
  }

  "2PC commit" must {
    val tid = "44"

    "fail if prepare has not been called" in {
      replicator ! TwoPhaseCommitCommit(tid)
      expectMsg(
        TwoPhaseCommitCommitError(
          "no transaction with id " + tid + ": prepare not called or wrong transaction id",
          None))
    }

    "succeed with an empty transaction" in {
      replicator ! TwoPhaseCommitPrepare(tid)
      expectMsg(TwoPhaseCommitPrepareSuccess(None))

      replicator ! TwoPhaseCommitCommit(tid)
      expectMsg(TwoPhaseCommitCommitSuccess(None))
    }

    "modify data after commit" in {
      val KeyA = GCounterKey("A")

      // call prepare
      replicator ! TwoPhaseCommitPrepare(tid)
      expectMsg(TwoPhaseCommitPrepareSuccess(None))

      // subscribe key
      val changedProbe = TestProbe()
      replicator ! Subscribe(KeyA, changedProbe.ref)
      replicator ! Get(KeyA, ReadLocal)
      expectMsg(NotFound(KeyA, None))

      // update key
      val c3 = GCounter() :+ 3

      replicator ! Update(KeyA, GCounter(), WriteLocal, None, Option(tid))(_ :+ 3)
      expectMsg(UpdateSuccess(KeyA, None))

      // get with transaction context
      replicator ! Get(KeyA, ReadLocal, None, Option(tid))
      expectMsg(GetSuccess(KeyA, None)(c3)).dataValue should be(c3)

      // get without transaction context
      replicator ! Get(KeyA, ReadLocal)
      expectMsg(NotFound(KeyA, None))

      // commit
      replicator ! TwoPhaseCommitCommit(tid)
      expectMsg(TwoPhaseCommitCommitSuccess(None))
      changedProbe.expectMsg(Changed(KeyA)(c3)).dataValue should be(c3)

      // second commit should fail
      replicator ! TwoPhaseCommitCommit(tid)
      expectMsg(
        TwoPhaseCommitCommitError(
          "no transaction with id " + tid + ": prepare not called or wrong transaction id",
          None))

      // TODO Get
    }
  }

  "Transaction" must {

    "generate a correct id" in {
      val t1 = new Transaction(replicator, testActor, (_) => None)
      t1.id shouldBe a[TransactionId]
      t1.id should not be None

      val t2 = new Transaction(replicator, testActor, (_) => None)
      t1.id should not be None

      (t1.id should not).equal(t2.id)
    }

    "commit an empty without error when empty" in {
      val t1 = new Transaction(replicator, testActor, (_) => {
      })
      t1.commit() should be(true)

      val t2 = new Transaction(replicator, testActor, (_) => None)
      t2.commit() should be(true)
    }

    "replicate updates" in {
      val KEY = FlagKey("F")
      var f1 = Flag()

      join(first, first)
      join(second, first)

      runOn(first, second) {
        within(20.seconds) {
          awaitAssert {
            replicator ! GetReplicaCount
            expectMsg(ReplicaCount(2))
          }
        }
      }

      enterBarrier("2-nodes")

      runOn(first) {
        f1.enabled should be(false)

        val t1 = new Transaction(replicator, testActor, (ctx) => {
          f1.enabled should be(false)
          f1 = f1.switchOn
          f1.enabled should be(true)

          // update
          //        ctx.update(KEY)(_.get.switchOn)
          ctx.update(KEY)(f1)
          expectMsg(UpdateSuccess(KEY, None))

          // read own write
          ctx.get(KEY)
          expectMsg(GetSuccess(KEY, None)(f1)).dataValue should be(f1)
        })

        t1.commit() should be(true)

        // local read from second transaction
        val t2 = new Transaction(replicator, testActor, (ctx) => {
          ctx.get(KEY)
          expectMsg(GetSuccess(KEY, None)(f1)).dataValue should be(f1)
        })

        t2.commit() should be(true)
      }

      enterBarrier("update flag")

      runOn(second) {
        val t = new Transaction(replicator, testActor, (ctx) => {

          within(5.seconds) {
            awaitAssert {
              ctx.get(KEY)
              expectMsg(GetSuccess(KEY, None)(f1))
            }
          }
        })

        t.commit() should be(true)
      }

      enterBarrierAfterTestStep()
    }

//    "work in single node cluster" in {
//      join(first, first)
//
//      runOn(first) {
//
//        within(5.seconds) {
//          awaitAssert {
//            replicator ! GetReplicaCount
//            expectMsg(ReplicaCount(1))
//          }
//        }
//
//        var f1 = Flag()
//        f1.enabled should be(false)
//
//        val t1 = new Transaction(replicator, () => {
//          println("Transaction 1")
//
//          f1.enabled should be(false)
//          f1 = f1.switchOn
//          f1.enabled should be(true)
//
//          val f2 = Flag()
//          f2.enabled should be(false)
//        })
//
//        f1.enabled should be(false)
//        t1.commit() should be(true) // commit here
////        expectMsg(PrepareCommitSuccess(None))
//
//        f1.enabled should be(true)
//      }
//
//      enterBarrierAfterTestStep()
//    }
  }

}
