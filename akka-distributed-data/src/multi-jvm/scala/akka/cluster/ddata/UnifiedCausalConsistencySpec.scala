/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.actor.{ActorIdentity, ActorRef, Deploy, Identify, Props}
import akka.cluster.Cluster
import akka.cluster.ddata.Replicator.{GetReplicaCount, ReplicaCount}
import akka.remote.testconductor.RoleName
import akka.remote.testkit.{MultiNodeConfig, MultiNodeSpec}
import akka.remote.transport.ThrottlerTransportAdapter.Direction
import akka.testkit.{DefaultTimeout, ImplicitSender}
import com.typesafe.config.ConfigFactory
import org.scalatest.Suite

import scala.concurrent.duration.DurationInt

object UnifiedCausalConsistencyMultiJvmSpec extends MultiNodeConfig {
  commonConfig(debugConfig(on = true).withFallback(ConfigFactory.parseString("""
      akka.loglevel = INFO
      akka.actor.provider = "cluster"
      akka.actor.allow-java-serialization = true
      akka.remote.artery.enabled = false
      akka.cluster.distributed-data.gossip-interval = 1s
    """)))

  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")

  testTransport(on = true) // needed for throttle
}

class UnifiedCausalConsistencyMultiJvmNode1 extends UnifiedCausalConsistencySpec //(ConfigFactory.parseString("akka.remote.artery.canonical.port = 2553"))
class UnifiedCausalConsistencyMultiJvmNode2 extends UnifiedCausalConsistencySpec //(ConfigFactory.parseString("akka.remote.artery.canonical.port = 2554"))
class UnifiedCausalConsistencyMultiJvmNode3 extends UnifiedCausalConsistencySpec //(ConfigFactory.parseString("akka.remote.artery.canonical.port = 2555"))

final case class MyCausalMessage(replyTo: ActorRef, var i: Int) {}

final case class MyNonCausalMessage(replyTo: ActorRef) {}

final case class Ack(var i: Int) {}

object MyCausalActor {
  def props(onCausalMsg: (MyCausalMessage) => Unit): Props = Props(new MyCausalActor(onCausalMsg))
}

final case class MyCausalActor(var onCausalMsg: (MyCausalMessage) => Unit) extends CausalActor {
  println(">> MyCausalActor::MyCausalActor()")

  /**
   * Scala API: This defines the initial actor behavior, it must return a partial function
   * with the actor logic.
   */
  override def receive: Receive = {
    super.receive.orElse({
      case msg: MyNonCausalMessage =>
        println("MyCausalActor::receive(msg=" + msg + ")")
        msg.replyTo ! Ack(-1)
      case msg: MyCausalMessage =>
        println("===== MyCausalActor::receive(msg=" + msg + ")")
//        msg.replyTo ! Ack(msg.i)
        onCausalMsg(msg)
    })
  }

  override def onCausalChange(gssVV: VersionVector): Unit = {
    println("MyCausalActor::onCausalChange(gssVV=" + gssVV + ")")
  }
}

class UnifiedCausalConsistencySpec
    extends MultiNodeSpec(UnifiedCausalConsistencyMultiJvmSpec /*, c => ActorSystem("toto", c.withFallback(conf))*/ )
    with Suite
    with STMultiNodeSpec
    with ImplicitSender
    with DefaultTimeout {

  import UnifiedCausalConsistencyMultiJvmSpec._

  override def initialParticipants = roles.size

  lazy val echo = {
    system.actorSelection(node(node1) / "user" / "echo") ! Identify(None)
    expectMsgType[ActorIdentity].ref.get
  }

  val cluster = Cluster(system)
//  implicit val selfUniqueAddress: SelfUniqueAddress = DistributedData(system).selfUniqueAddress
  val replicator = DistributedData(system).replicator

  var afterCounter = 0
  def enterBarrierAfterTestStep(): Unit = {
    afterCounter += 1
    enterBarrier("after-" + afterCounter)
  }

//  private implicit val askTimeout: Timeout = 10.seconds

  def join(from: RoleName, to: RoleName): Unit = {
    runOn(from) {
      cluster.join(node(to).address)
    }
    enterBarrier(from.name + "-joined")
  }

  "Unified Causal Consistency" must {

//    "detect causal messages" in {
//      runOn(node1) {
//        val causalActorRef = system.actorOf(MyCausalActor.props((_, msg) => {
//          msg.replyTo ! Ack(msg.i)
//        }), "MyCausalActor")
//
//        println(">> send MyNonCausalMessage")
//        causalActorRef ! MyNonCausalMessage(testActor)
//        expectMsg(Ack(-1))
//
//        println(">> send MyCausalMessage without causalTell")
//        causalActorRef ! MyCausalMessage(testActor, 42)
//        expectMsg(Ack(42))
//
//        println(">> send MyCausalMessage with causalTell")
//        val causalContext =
//          new Metadata(VersionVector.empty, mutable.Map(causalActorRef -> mutable.Map(testActor -> 0)))
//
//        causalActorRef.causalTell(MyCausalMessage(testActor, 43), causalContext, testActor)
//        expectMsg(Ack(43))
//      }
//      enterBarrier("after")
//    }

    "correctly delay causal messages" in {
      join(node1, node1)
      join(node2, node1)
      join(node3, node1)

      runOn(node1, node2, node3) {
        within(10.seconds) {
          awaitAssert {
            replicator ! GetReplicaCount
            expectMsg(ReplicaCount(3))
          }
        }
      }

      enterBarrier("3-nodes")

      var myCausalActorA = ActorRef.noSender
      var myCausalActorB = ActorRef.noSender
      var myCausalActorC = ActorRef.noSender

      runOn(node3) {
        val arr = new Array[Int](10)

        /**
         * ACTOR C
         */
        myCausalActorC = system.actorOf(
          MyCausalActor
            .props((msg) => {
              arr(msg.i) = arr(msg.i) + 1

              if (2 == arr(msg.i)) {
                println(">> SENDING ACK to " + msg.replyTo)
                msg.replyTo ! Ack(msg.i)
              }
            })
            .withDeploy(Deploy.local),
          "MyCausalActorC")
      }

      enterBarrier("startup - node3")

      runOn(node2) {
        system.actorSelection(node(node3) / "user" / "MyCausalActorC") ! Identify(None)
        myCausalActorC = expectMsgType[ActorIdentity](5.seconds).ref.get

        myCausalActorC should not be ActorRef.noSender

        /**
         * ACTOR B
         */
        myCausalActorB = system.actorOf(
          MyCausalActor
            .props((msg) => {
              println(msg)

              new Transaction(system, myCausalActorB, (ctx) => {
                ctx.causalTell(msg, myCausalActorC)
              }).commit()

//              myCausalActorC.causalTell(MyCausalMessage(msg.replyTo, msg.i), causalContext, myCausalActorB)
            })
            .withDeploy(Deploy.local),
          "MyCausalActorB")
      }

      enterBarrier("startup - node2")

      runOn(node1) {
        system.actorSelection(node(node3) / "user" / "MyCausalActorC") ! Identify(None)
        myCausalActorC = expectMsgType[ActorIdentity](5.seconds).ref.get
        system.actorSelection(node(node2) / "user" / "MyCausalActorB") ! Identify(None)
        myCausalActorB = expectMsgType[ActorIdentity](5.seconds).ref.get

        myCausalActorC should not be ActorRef.noSender
        myCausalActorC should not be testActor
        myCausalActorB should not be ActorRef.noSender
        myCausalActorB should not be testActor

        // throttle A to C
        testConductor.throttle(node1, node3, Direction.Send, 1).await

        /**
         * ACTOR A
         */
        myCausalActorA = system.actorOf(
          MyCausalActor
            .props((msg) => {
              println("SENDING MESSAGES !! msg=" + msg)

              new Transaction(system, myCausalActorA, (ctx) => {
                println(">>>>>>>>>>>>>>>>>>>>>>> ctx=" + ctx)
                ctx.causalTell(msg, myCausalActorC)
                ctx.causalTell(msg, myCausalActorB)
              }).commit()

//              myCausalActorC.causalTell(MyCausalMessage(msg.replyTo, msg.i), causalContext, myCausalActorA)
//              myCausalActorB.causalTell(MyCausalMessage(msg.replyTo, msg.i), causalContext, myCausalActorA)
            })
            .withDeploy(Deploy.local),
          "MyCausalActorA")
      }

      enterBarrier("startup - node1")

      runOn(node1) {
        system.actorSelection(node(node1) / "user" / "MyCausalActorA") ! Identify(None)
        myCausalActorA = expectMsgType[ActorIdentity](5.seconds).ref.get

        myCausalActorC should not be ActorRef.noSender
        myCausalActorB should not be ActorRef.noSender
        myCausalActorA should not be ActorRef.noSender

        for (i <- 0 until 10) {
          println("> SENDING MESSAGE i=" + i + " from " + self + " to " + myCausalActorA)

//          new Transaction(replicator, self, (ctx) => {
//            ctx.causalTell(MyCausalMessage(testActor, i), myCausalActorA)
//          }).commit()

          myCausalActorA ! MyCausalMessage(testActor, i)

//          myCausalActorA.causalTell(MyCausalMessage(testActor, i), causalContext, testActor)

          expectMsgType[Ack](30.seconds).i should be(i)
        }
      }

      enterBarrier("after")
    }

//    "send causal message to self" in {
//      new Transaction(replicator, testActor, (ctx) => {
//        ctx.causalTell(MyCausalMessage(testActor, 42), testActor)
//      }).commit()
//
//      expectMsg(5.seconds, MyCausalMessage(testActor, 42))
//    }

  }

}
