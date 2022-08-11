/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.actor.{ActorIdentity, ActorRef, Deploy, Identify, Metadata, Props}
import akka.cluster.Cluster
import akka.remote.RemotingMultiNodeSpec
import akka.remote.testconductor.RoleName
import akka.remote.testkit.MultiNodeConfig
import akka.remote.transport.ThrottlerTransportAdapter.Direction
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration.DurationInt

object UnifiedCausalConsistencyMultiJvmSpec extends MultiNodeConfig {
  commonConfig(debugConfig(on = true).withFallback(ConfigFactory.parseString("""
      akka.loglevel = INFO
      akka.actor.provider = "cluster"
      akka.actor.allow-java-serialization = true
      akka.remote.artery.enabled = false
    """)).withFallback(RemotingMultiNodeSpec.commonConfig))

  val node1 = role("node1")
  val node2 = role("node2")
  val node3 = role("node3")

  testTransport(on = true)
}

class UnifiedCausalConsistencyMultiJvmNode1 extends UnifiedCausalConsistencySpec
class UnifiedCausalConsistencyMultiJvmNode2 extends UnifiedCausalConsistencySpec
class UnifiedCausalConsistencyMultiJvmNode3 extends UnifiedCausalConsistencySpec

final case class MyCausalMessage(replyTo: ActorRef, var i: Int) {}

final case class MyNonCausalMessage(replyTo: ActorRef) {}

final case class Ack(var i: Int) {}

object MyCausalActor {
  def props(onCausalMsg: (Metadata, MyCausalMessage) => Unit): Props = Props(new MyCausalActor(onCausalMsg))
}

final case class MyCausalActor(var onCausalMsg: (Metadata, MyCausalMessage) => Unit) extends CausalActor {
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
        onCausalMsg(causalContext, msg)
    })
  }

  override def onCausalChange(causalContext: Metadata): Unit = {
    println("MyCausalActor::onCausalChange(causalContext=" + causalContext + ")")
  }
}

class UnifiedCausalConsistencySpec extends RemotingMultiNodeSpec(UnifiedCausalConsistencyMultiJvmSpec) {

  import UnifiedCausalConsistencyMultiJvmSpec._

  override def initialParticipants = 2

  lazy val echo = {
    system.actorSelection(node(node1) / "user" / "echo") ! Identify(None)
    expectMsgType[ActorIdentity].ref.get
  }

  val cluster = Cluster(system)
  implicit val selfUniqueAddress: SelfUniqueAddress = DistributedData(system).selfUniqueAddress
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
      enterBarrier("startup")

      var myCausalActorA = ActorRef.noSender
      var myCausalActorB = ActorRef.noSender
      var myCausalActorC = ActorRef.noSender

      runOn(node3) {
        val arr = new Array[Int](10)

        /**
         * ACTOR C
         */
        system.actorOf(
          MyCausalActor
            .props((_, msg) => {
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
            .props((causalContext, msg) => {
              println(msg)
              myCausalActorC.causalTell(MyCausalMessage(msg.replyTo, msg.i), causalContext, myCausalActorB)
//              expectMsg(5.seconds, Ack(msg.i))
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
        myCausalActorB should not be ActorRef.noSender

        // throttle A to C
        testConductor.throttle(node1, node3, Direction.Send, 1).await

        /**
         * ACTOR A
         */
        myCausalActorA = system.actorOf(
          MyCausalActor
            .props((causalContext, msg) => {
              println("SENDING MESSAGES !! causalContext.matrix=" + causalContext.lastDeliveredSequenceMatrix)

              myCausalActorC.causalTell(MyCausalMessage(msg.replyTo, msg.i), causalContext, myCausalActorA)
              myCausalActorB.causalTell(MyCausalMessage(msg.replyTo, msg.i), causalContext, myCausalActorA)

//              msg.replyTo ! Ack(msg.i) // reply to test actor
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
          val causalContext =
            new Metadata(VersionVector.empty, Map(myCausalActorA -> Map(testActor -> i)))

          myCausalActorA.causalTell(MyCausalMessage(testActor, i), causalContext, testActor)

          expectMsgType[Ack](5.seconds).i should be(i)
        }
      }

      enterBarrier("after")
    }

    "correctly delay causal messages and shared memory" in {

    }

  }

}
