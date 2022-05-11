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

  "prepare" must {
    val tid = "41"

    "return a correct version vector" in {
      replicator ! TwoPhaseCommitPrepare(tid)
      expectMsg(TwoPhaseCommitPrepareSuccess(VersionVector(selfUniqueAddress.uniqueAddress, 0), None))
    }

    "handle already existing transaction" in {
      replicator ! TwoPhaseCommitPrepare(tid)
      expectMsg(TwoPhaseCommitPrepareError("Transaction id " + tid + " already inflight", None))
    }
  }

  "abort" must {
    "succeed without prior prepare" in {
      replicator ! TwoPhaseCommitAbort("42")
      expectMsg(TwoPhaseCommitAbortSuccess(None))
    }

    "succeed with prior prepare while transaction is empty" in {
      val tid = "42"

      replicator ! TwoPhaseCommitPrepare(tid)
      expectMsg(TwoPhaseCommitPrepareSuccess(VersionVector(selfUniqueAddress.uniqueAddress, 0), None))

      replicator ! TwoPhaseCommitAbort(tid)
      expectMsg(TwoPhaseCommitAbortSuccess(None))
    }

    "not modify data after abort" in {
      val KeyB = GCounterKey("B")
      val ctx = Transaction.Context(replicator, testActor)

      // prepare
      replicator ! TwoPhaseCommitPrepare(ctx.tid)
      expectMsg(TwoPhaseCommitPrepareSuccess(VersionVector(selfUniqueAddress.uniqueAddress, 0), None))

      // KeyA should not be found
      replicator ! Get(KeyB, ReadLocal)
      expectMsg(NotFound(KeyB, None))

      // update key
      val c3 = GCounter() :+ 3

      replicator ! Update(KeyB, GCounter(), WriteLocal, None, Some(ctx.tid))(_ :+ 3)
      expectMsg(UpdateSuccess(KeyB, None))

      // get with transaction context
      replicator ! Get(KeyB, ReadLocal, None, Some(ctx))
      expectMsg(GetSuccess(KeyB, None)(c3)).dataValue should be(c3)

      // get without transaction context
      replicator ! Get(KeyB, ReadLocal)
      expectMsg(NotFound(KeyB, None))

      // abort
      replicator ! TwoPhaseCommitAbort(ctx.tid)
      expectMsg(TwoPhaseCommitAbortSuccess(None))

      // get with transaction context
      replicator ! Get(KeyB, ReadLocal, None, Some(ctx))
      expectMsg(NotFound(KeyB, None))

      // get without transaction context
      replicator ! Get(KeyB, ReadLocal)
      expectMsg(NotFound(KeyB, None))
    }
  }

  "commit" must {
    val ctx = Transaction.Context(replicator, testActor)

    "fail if prepare has not been called" in {
      replicator ! TwoPhaseCommitCommit(ctx)
      expectMsg(
        TwoPhaseCommitCommitError(
          "no transaction with id " + ctx.tid + ": prepare not called or wrong transaction id",
          None))
    }

    "succeed with an empty transaction" in {
      replicator ! TwoPhaseCommitPrepare(ctx.tid)
      expectMsg(TwoPhaseCommitPrepareSuccess(VersionVector(selfUniqueAddress.uniqueAddress, 0), None))

      replicator ! TwoPhaseCommitCommit(ctx)
      expectMsg(TwoPhaseCommitCommitSuccess(None))
    }

    "modify data correctly" in {
      val KeyA = GCounterKey("A")

      // call prepare
      replicator ! TwoPhaseCommitPrepare(ctx.tid)
      expectMsg(TwoPhaseCommitPrepareSuccess(VersionVector(selfUniqueAddress.uniqueAddress, 0), None))

      // update key
      val c3 = GCounter() :+ 3

      replicator ! Update(KeyA, GCounter(), WriteLocal, None, Some(ctx.tid))(_ :+ 3)
      expectMsg(UpdateSuccess(KeyA, None))

      // get with transaction context
      replicator ! Get(KeyA, ReadLocal, None, Some(ctx))
      expectMsg(GetSuccess(KeyA, None)(c3)).dataValue should be(c3)

      // get without transaction context
      replicator ! Get(KeyA, ReadLocal)
      expectMsg(NotFound(KeyA, None))

      // commit
      replicator ! TwoPhaseCommitCommit(ctx)
      expectMsg(TwoPhaseCommitCommitSuccess(None))

      // second commit should fail
      replicator ! TwoPhaseCommitCommit(ctx)
      expectMsg(
        TwoPhaseCommitCommitError(
          "no transaction with id " + ctx.tid + ": prepare not called or wrong transaction id",
          None))

      // get from a second context
      val ctx2 = Transaction.Context(replicator, testActor)
      // prepare
      replicator ! TwoPhaseCommitPrepare(ctx2.tid)
      expectMsg(TwoPhaseCommitPrepareSuccess(VersionVector(selfUniqueAddress.uniqueAddress, 1), None))
      // get with transaction context
      replicator ! Get(KeyA, ReadLocal, None, Some(ctx2))
      expectMsg(GetSuccess(KeyA, None)(c3)).dataValue should be(c3)
      // commit
      replicator ! TwoPhaseCommitCommit(ctx2)
      expectMsg(TwoPhaseCommitCommitSuccess(None))
    }
  }

  "Transaction" must {

    "instantiate correctly" in {
      val t1 = new Transaction(replicator, testActor, (_) => None)
      t1.id shouldBe a[TransactionId]
      t1.id should not be None
      t1.context.tid should not be (None)
//      t1.context.version should be(VersionVector.empty)
      t1.context.replicator should be(replicator)
      t1.context.actor should be(testActor)

      val t2 = new Transaction(replicator, testActor, (_) => None)
      t1.id should not be None
      (t1.id should not).equal(t2.id)
    }

    "commit without error when no operations" in {
      val t1 = new Transaction(replicator, testActor, (_) => {})
      t1.commit() should be(true)

      val t2 = new Transaction(replicator, testActor, (_) => None)
      t2.commit() should be(true)
    }

    // TODO: handle exception in operations
    // TODO: delete key

    "abort without error when no operations" in {
      val t1 = new Transaction(replicator, testActor, (_) => {})
      t1.abort() // TODO: what should be asserted ?

      val t2 = new Transaction(replicator, testActor, (_) => None)
      t2.abort()
    }

    "read own write in same transaction on same node" in {
      val t1 = new Transaction(replicator, testActor, (ctx) => {
        val KEY = FlagKey("F1")
        var f1 = Flag()

        f1.enabled should be(false)
        f1 = f1.switchOn
        f1.enabled should be(true)

        // update
        ctx.update(KEY)(f1)
        expectMsg(UpdateSuccess(KEY, None))

        // read own write
        ctx.get(KEY)
        expectMsg(GetSuccess(KEY, None)(f1)).dataValue should be(f1)
      })

      t1.commit() should be(true)
    }

    "read own write in second transaction on same node" in {
      val t1 = new Transaction(replicator, testActor, (ctx) => {
        val KEY = FlagKey("F1")
        val f1 = Flag().switchOn

        // read own write
        ctx.get(KEY)
        expectMsg(GetSuccess(KEY, None)(f1)).dataValue should be(f1)
      })

      t1.commit() should be(true)
    }

    "replicate updates correctly on multiple nodes" in {
      val KEY = FlagKey("F2")
      val flag = Flag().switchOn

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

      /*
        On first node, write data
       */
      runOn(first) {
        val t1 = new Transaction(replicator, testActor, (ctx) => {
          // update
          ctx.update(KEY)(flag)
          expectMsg(UpdateSuccess(KEY, None))

          // read own write
          ctx.get(KEY)
          expectMsg(GetSuccess(KEY, None)(flag)).dataValue should be(flag)
        })
        t1.commit() should be(true)

        // local read from previous transaction
        val t2 = new Transaction(replicator, testActor, (ctx) => {
          ctx.get(KEY)
          expectMsg(GetSuccess(KEY, None)(flag)).dataValue should be(flag)
        })
        t2.commit() should be(true)
      }

      enterBarrier("update flag")

      /*
        On second node, wait for replication and read
       */
      runOn(second) {
        within(10.seconds) {
          awaitAssert({
            val t = new Transaction(replicator, testActor, (ctx) => {
              ctx.get(KEY)
              expectMsg(GetSuccess(KEY, None)(flag))
            })
            t.commit() should be(true)
          }, interval = 1.second)
        }
      }

      enterBarrierAfterTestStep()
    }

    "converge after many concurrent updates" in within(30.seconds) {
      join(third, first)
      join(third, second)

      runOn(first, second, third) {
        within(10.seconds) {
          awaitAssert {
            replicator ! GetReplicaCount
            expectMsg(ReplicaCount(3))
          }
        }
      }
      enterBarrier("3-nodes")

      val KeyG = GCounterKey("G")

      runOn(first, second, third) {
        var c = GCounter()

        val t = new Transaction(replicator, testActor, (ctx) => {
          for (_ <- 0 until 100) {
            c :+= 1
            ctx.update(KeyG)(c)
          }
          val results = receiveN(100)
          results.map(_.getClass).toSet should be(Set(classOf[UpdateSuccess[_]]))
        })
        t.commit() should be(true)
      }
      enterBarrier("100-updates-done")

      runOn(first, second, third) {
        within(20.seconds) {
          awaitAssert({
            val t = new Transaction(replicator, testActor, (ctx) => {
              ctx.get(KeyG)
              val c = expectMsgPF() { case g @ GetSuccess(KeyG, _) => g.get(KeyG) }
              c.value should be(3 * 100)
            })
            t.commit() should be(true)
          }, interval = 5.second)
        }
      }
      enterBarrierAfterTestStep()
    }

    "maintain causality" in {
      // TODO
    }

  }

}
