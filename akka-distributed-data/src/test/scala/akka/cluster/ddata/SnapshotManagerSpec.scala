/*
 * Copyright (C) 2021-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.actor.Address
import akka.cluster.UniqueAddress
import akka.cluster.ddata.Replicator.Internal.DataEnvelope
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.TreeMap

class SnapshotManagerSpec extends AnyWordSpec with Matchers {
  val node1 = UniqueAddress(Address("akka", "Sys", "localhost", 2551), 1L)
  val node2 = UniqueAddress(node1.address.copy(port = Some(2552)), 2L)
  val node3 = UniqueAddress(node1.address.copy(port = Some(2553)), 3L)
  val node4 = UniqueAddress(node1.address.copy(port = Some(2554)), 4L)

  implicit val selfUniqueAddress: SelfUniqueAddress = SelfUniqueAddress(node1)

  "SnapshotManager" must {
//    val dataEntries: DataEntries = Map.empty[KeyId, DataEnvelope]
    val key1 = "key1"
    val key2 = "key2"
    val c1 = GCounter() :+ 1
    val c2 = GCounter() :+ 2
    val c3 = GCounter() :+ 3
    val c4 = GCounter() :+ 4

    val vv0 = ManyVersionVector(TreeMap(node1 -> 0))
    val vv1 = ManyVersionVector(TreeMap(node1 -> 1))
    val vv2 = ManyVersionVector(TreeMap(node1 -> 2))
    val vv11 = ManyVersionVector(TreeMap(node1 -> 1, node2 -> 1))
    val vv21 = ManyVersionVector(TreeMap(node1 -> 2, node2 -> 1))
    val vv22 = ManyVersionVector(TreeMap(node1 -> 2, node2 -> 2))

    val snapshotManager = SnapshotManager(node1)

    "get and update without prior prepare correctly" in {
      // update
      snapshotManager.update("NOT A TID", key1, DataEnvelope(c1))
      // check currentTransactions
      snapshotManager.currentTransactions.size should be(0)
      // check committedTransactions
//      snapshotManager.lastestLocalSnapshot._1.compareTo(VersionVector.empty) should be(VersionVector.Same)
//      snapshotManager.lastestLocalSnapshot._2.size should be(0)
      // check knownVersionVectors
      snapshotManager.getKnownVectorClocks.size should be(0)
      // check globalStableSnapshot
      snapshotManager.globalStableSnapshot._1.compareTo(vv0) should be(VersionVector.Same)
      snapshotManager.globalStableSnapshot._2.size should be(0)

      // get
      snapshotManager.get("NOT A TID", key1) should be(None)
      // check currentTransactions
      snapshotManager.currentTransactions.size should be(0)
      // check committedTransactions
//      snapshotManager.lastestLocalSnapshot._1.compareTo(VersionVector.empty) should be(VersionVector.Same)
//      snapshotManager.lastestLocalSnapshot._2.size should be(0)
      // check knownVersionVectors
      snapshotManager.getKnownVectorClocks.size should be(0)
      // check globalStableSnapshot
      snapshotManager.globalStableSnapshot._1.compareTo(vv0) should be(VersionVector.Same)
      snapshotManager.globalStableSnapshot._2.size should be(0)
    }

    "update multiple times correctly" in {
      val tid = "42"
      val (vv, _) = snapshotManager.transactionPrepare(tid)

      snapshotManager.update(tid, key1, DataEnvelope(c1))
      // check currentTransactions
      snapshotManager.currentTransactions.contains(tid) should be(true)
      snapshotManager.currentTransactions(tid)._1._1.compareTo(vv) should be(VersionVector.Same)
      snapshotManager.currentTransactions(tid)._1._2(key1).data should be(c1)
      snapshotManager.currentTransactions(tid)._2 should be(true)
      // check committedTransactions
      snapshotManager.localSnapshots.size should be(0)
      // check knownVersionVectors
      snapshotManager.getKnownVectorClocks.size should be(0)
      // check globalStableSnapshot
      snapshotManager.globalStableSnapshot._1.compareTo(vv0) should be(VersionVector.Same)
      snapshotManager.globalStableSnapshot._2.size should be(0)

      // same data
      snapshotManager.update(tid, key1, DataEnvelope(c1))
      // check currentTransactions
      snapshotManager.currentTransactions.contains(tid) should be(true)
      snapshotManager.currentTransactions(tid)._1._1.compareTo(vv) should be(VersionVector.Same)
      snapshotManager.currentTransactions(tid)._1._2(key1).data should be(c1)
      snapshotManager.currentTransactions(tid)._2 should be(true)
      // check committedTransactions
      snapshotManager.localSnapshots.size should be(0)
      // check knownVersionVectors
      snapshotManager.getKnownVectorClocks.size should be(0)
      // check globalStableSnapshot
      snapshotManager.globalStableSnapshot._1.compareTo(vv0) should be(VersionVector.Same)
      snapshotManager.globalStableSnapshot._2.size should be(0)

      // different key
      snapshotManager.update(tid, key2, DataEnvelope(c1))
      // check currentTransactions
      snapshotManager.currentTransactions.contains(tid) should be(true)
      snapshotManager.currentTransactions(tid)._1._1.compareTo(vv) should be(VersionVector.Same)
      snapshotManager.currentTransactions(tid)._1._2(key1).data should be(c1)
      snapshotManager.currentTransactions(tid)._1._2(key2).data should be(c1)
      snapshotManager.currentTransactions(tid)._2 should be(true)
      // check committedTransactions
      snapshotManager.localSnapshots.size should be(0)
      // check knownVersionVectors
      snapshotManager.getKnownVectorClocks.size should be(0)
      // check globalStableSnapshot
      snapshotManager.globalStableSnapshot._1.compareTo(vv0) should be(VersionVector.Same)
      snapshotManager.globalStableSnapshot._2.size should be(0)

      // same key, different value
      snapshotManager.update(tid, key2, DataEnvelope(c2))
      // check currentTransactions
      snapshotManager.currentTransactions.contains(tid) should be(true)
      snapshotManager.currentTransactions(tid)._1._1.compareTo(vv) should be(VersionVector.Same)
      snapshotManager.currentTransactions(tid)._1._2(key1).data should be(c1)
      snapshotManager.currentTransactions(tid)._1._2(key2).data should be(c2)
      snapshotManager.currentTransactions(tid)._2 should be(true)
      // check committedTransactions
      snapshotManager.localSnapshots.size should be(0)
      // check knownVersionVectors
      snapshotManager.getKnownVectorClocks.size should be(0)
      // check globalStableSnapshot
      snapshotManager.globalStableSnapshot._1.compareTo(vv0) should be(VersionVector.Same)
      snapshotManager.globalStableSnapshot._2.size should be(0)

      // commit
      val commitVv = snapshotManager.commit(tid)
      commitVv.compareTo(vv) should be(VersionVector.After)
      // check currentTransactions
      snapshotManager.currentTransactions.contains(tid) should be(false)
      // check committedTransactions
      snapshotManager.localSnapshots.size should be(1)
      snapshotManager.localSnapshots.last._1.compareTo(commitVv) should be(VersionVector.Same)
      snapshotManager.localSnapshots.last._2.size should be(2)
      snapshotManager.localSnapshots.last._2(key1).data should be(c1)
      snapshotManager.localSnapshots.last._2(key2).data should be(c2)
      // check knownVersionVectors
      snapshotManager.getKnownVectorClocks.size should be(0)
      // check globalStableSnapshot
      snapshotManager.globalStableSnapshot._1.compareTo(vv0) should be(VersionVector.Same)
      snapshotManager.globalStableSnapshot._2.size should be(0)
    }

    "get previously added key with correct vector clock" in {
      val tid = "43"
      val (vv, _) = snapshotManager.transactionPrepare(tid)

      vv.compareTo(OneVersionVector(node1, 1)) should be(VersionVector.Same)

      snapshotManager.get(tid, key1) should be(Some(c1))
      snapshotManager.get(tid, key2) should be(Some(c2))

      // commit
      val commitVv = snapshotManager.commit(tid)
      commitVv.compareTo(vv) should be(VersionVector.Same)

      // check currentTransactions
      snapshotManager.currentTransactions.contains(tid) should be(false)
      // check committedTransactions
      snapshotManager.localSnapshots.size should be(1)
      snapshotManager.localSnapshots.last._1.compareTo(commitVv) should be(VersionVector.Same)
      snapshotManager.localSnapshots.last._2.size should be(2)
      snapshotManager.localSnapshots.last._2(key1).data should be(c1)
      snapshotManager.localSnapshots.last._2(key2).data should be(c2)
      // check knownVersionVectors
      snapshotManager.getKnownVectorClocks.size should be(0)
      // check globalStableSnapshot
      snapshotManager.globalStableSnapshot._1.compareTo(vv0) should be(VersionVector.Same)
      snapshotManager.globalStableSnapshot._2.size should be(0)
    }

    "get unknown key" in {
      val tid = "44"
      val (_, _) = snapshotManager.transactionPrepare(tid)
      val key = "unknown key"
      snapshotManager.get(tid, key) should be(None)
      snapshotManager.abort(tid)
    }

    "update GSS correctly" in {
      // empty KnownVectorClocks
      snapshotManager.getKnownVectorClocks.size should be(0)
      snapshotManager.globalStableSnapshot._1.compareTo(OneVersionVector(selfUniqueAddress.uniqueAddress, 0)) should be(
        VersionVector.Same)

      // update node1
      snapshotManager.updateKnownVersionVectors(node1, vv1)
      snapshotManager.getKnownVectorClocks should be(Map(node1 -> vv1))
      snapshotManager.globalStableSnapshot._1.compareTo(vv1) should be(VersionVector.Same)
      snapshotManager.globalStableSnapshot._2.size should be(2)
      snapshotManager.globalStableSnapshot._2(key1).data should be(c1)
      snapshotManager.globalStableSnapshot._2(key2).data should be(c2)
      snapshotManager.localSnapshots.size should be(0)

      // update node2
      snapshotManager.updateKnownVersionVectors(node2, vv11)
      snapshotManager.getKnownVectorClocks should be(Map(node1 -> vv1, node2 -> vv11))
      snapshotManager.globalStableSnapshot._1.compareTo(vv1) should be(VersionVector.Same)
      snapshotManager.globalStableSnapshot._2.size should be(2)
      snapshotManager.globalStableSnapshot._2(key1).data should be(c1)
      snapshotManager.globalStableSnapshot._2(key2).data should be(c2)
      snapshotManager.localSnapshots.size should be(0)
    }

    "update GSS correctly after commit" in {
      snapshotManager.getKnownVectorClocks should be(Map(node1 -> vv1, node2 -> vv11))
      snapshotManager.globalStableSnapshot._1.compareTo(vv1) should be(VersionVector.Same)
      snapshotManager.globalStableSnapshot._2.size should be(2)
      snapshotManager.globalStableSnapshot._2(key1).data should be(c1)
      snapshotManager.globalStableSnapshot._2(key2).data should be(c2)
      snapshotManager.localSnapshots.size should be(0)

      // commit update
      val tid = "45"
      val (vv, _) = snapshotManager.transactionPrepare(tid)

      snapshotManager.update(tid, key1, DataEnvelope(c3))
      // check currentTransactions
      snapshotManager.currentTransactions.contains(tid) should be(true)
      snapshotManager.currentTransactions(tid)._1._1.compareTo(vv) should be(VersionVector.Same)
      snapshotManager.currentTransactions(tid)._1._2(key1).data should be(c3)
      snapshotManager.currentTransactions(tid)._2 should be(true)
      // check committedTransactions
      snapshotManager.localSnapshots.size should be(0)
      // check knownVersionVectors
      snapshotManager.getKnownVectorClocks should be(Map(node1 -> vv1, node2 -> vv11))
      // check globalStableSnapshot
      snapshotManager.globalStableSnapshot._1.compareTo(vv) should be(VersionVector.Same)
      snapshotManager.globalStableSnapshot._2.size should be(2)
      snapshotManager.globalStableSnapshot._2(key1).data should be(c1)
      snapshotManager.globalStableSnapshot._2(key2).data should be(c2)

      val commitVv = snapshotManager.commit(tid)
      commitVv.compareTo(vv2) should be(VersionVector.Same)

      // check currentTransactions
      snapshotManager.currentTransactions.contains(tid) should be(false)
      // check committedTransactions
      snapshotManager.localSnapshots.size should be(1)
      snapshotManager.localSnapshots.last._1.compareTo(commitVv) should be(VersionVector.Same)
      snapshotManager.localSnapshots.last._2.size should be(1)
      snapshotManager.localSnapshots.last._2(key1).data should be(c3)
      // check knownVersionVectors
      snapshotManager.getKnownVectorClocks should be(Map(node1 -> vv1, node2 -> vv11))
      // check globalStableSnapshot
      snapshotManager.globalStableSnapshot._1.compareTo(vv) should be(VersionVector.Same)
      snapshotManager.globalStableSnapshot._2.size should be(2)
      snapshotManager.globalStableSnapshot._2(key1).data should be(c1)
      snapshotManager.globalStableSnapshot._2(key2).data should be(c2)


      // updateKnownVersionVectors
      snapshotManager.updateKnownVersionVectors(node1, vv2)
      snapshotManager.getKnownVectorClocks should be(Map(node1 -> vv2, node2 -> vv11))
      snapshotManager.globalStableSnapshot._1.compareTo(vv1) should be(VersionVector.Same)
      snapshotManager.globalStableSnapshot._2.size should be(2)
      snapshotManager.globalStableSnapshot._2(key1).data should be(c1)
      snapshotManager.globalStableSnapshot._2(key2).data should be(c2)
      snapshotManager.localSnapshots.size should be(1)

      // updateKnownVersionVectors
      snapshotManager.updateKnownVersionVectors(node2, vv21)
      snapshotManager.getKnownVectorClocks should be(Map(node1 -> vv2, node2 -> vv21))
      snapshotManager.globalStableSnapshot._1.compareTo(vv2) should be(VersionVector.Same)
      snapshotManager.globalStableSnapshot._2.size should be(2)
      snapshotManager.globalStableSnapshot._2(key1).data should be(c3)
      snapshotManager.globalStableSnapshot._2(key2).data should be(c2)
      snapshotManager.localSnapshots.size should be(0)
    }

    "update from gossip correctly" in {
      // node 2 updates key1 with c4
      snapshotManager.updateFromGossip(vv22, Map(key1 -> DataEnvelope(c4)))
      // check currentTransactions
      snapshotManager.currentTransactions.size should be(0)
      // check committedTransactions
      snapshotManager.localSnapshots.size should be(1)
      snapshotManager.localSnapshots(vv22).size should be(1)
      snapshotManager.localSnapshots(vv22)(key1).data should be(c4)
      // check knownVersionVectors
      snapshotManager.getKnownVectorClocks should be(Map(node1 -> vv2, node2 -> vv21))  // only update knownVectorClocks on gossip without data
      // check globalStableSnapshot
      snapshotManager.globalStableSnapshot._1.compareTo(vv2) should be(VersionVector.Same)
      snapshotManager.globalStableSnapshot._2.size should be(2)
      snapshotManager.globalStableSnapshot._2(key1).data should be(c3)
      snapshotManager.globalStableSnapshot._2(key2).data should be(c2)


      // updateKnownVersionVectors node1 to vv22
      snapshotManager.updateKnownVersionVectors(node1, vv22)
      // check currentTransactions
      snapshotManager.currentTransactions.size should be(0)
      // check committedTransactions
      snapshotManager.localSnapshots.size should be(1)
      snapshotManager.localSnapshots(vv22).size should be(1)
      snapshotManager.localSnapshots(vv22)(key1).data should be(c4)
      // check knownVersionVectors
      snapshotManager.getKnownVectorClocks should be(Map(node1 -> vv22, node2 -> vv21))  // only update knownVectorClocks on gossip without data
      // check globalStableSnapshot
      snapshotManager.globalStableSnapshot._1.compareTo(vv21) should be(VersionVector.Same)
      snapshotManager.globalStableSnapshot._2.size should be(2)
      snapshotManager.globalStableSnapshot._2(key1).data should be(c3)
      snapshotManager.globalStableSnapshot._2(key2).data should be(c2)

      // updateKnownVersionVectors node2 to vv22
      snapshotManager.updateKnownVersionVectors(node2, vv22)
      // check currentTransactions
      snapshotManager.currentTransactions.size should be(0)
      // check committedTransactions
      snapshotManager.localSnapshots.size should be(0)
      // check knownVersionVectors
      snapshotManager.getKnownVectorClocks should be(Map(node1 -> vv22, node2 -> vv22))  // only update knownVectorClocks on gossip without data
      // check globalStableSnapshot
      snapshotManager.globalStableSnapshot._1.compareTo(vv22) should be(VersionVector.Same)
      snapshotManager.globalStableSnapshot._2.size should be(2)
      snapshotManager.globalStableSnapshot._2(key1).data should be(c4)
      snapshotManager.globalStableSnapshot._2(key2).data should be(c2)
    }
  }

}
