/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
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
//    val c3 = GCounter() :+ 3
//    val c4 = GCounter() :+ 4

    val vv0 = ManyVersionVector(TreeMap(node1 -> 0))
    val vv1 = ManyVersionVector(TreeMap(node1 -> 1))
    val vv4 = ManyVersionVector(TreeMap(node1 -> 1, node2 -> 1))
    val vv2 = ManyVersionVector(TreeMap(node1 -> 1, node2 -> 2))
    val vv3 = ManyVersionVector(TreeMap(node1 -> 2, node2 -> 2))
    val vv5 = ManyVersionVector(TreeMap(node1 -> 2, node2 -> 2, node3 -> 3))

    val snapshotManager = SnapshotManager(node1)


    "get and update without prior prepare correctly" in {
      // update
      snapshotManager.update("NOT A TID", key1, DataEnvelope(c1))
      // check currentTransactions
      snapshotManager.currentTransactions.size should be(0)
      // check committedTransactions
      snapshotManager.committedTransactions.size should be(0)
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
      snapshotManager.committedTransactions.size should be(0)
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
      snapshotManager.committedTransactions.size should be(0)
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
      snapshotManager.committedTransactions.size should be(0)
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
      snapshotManager.committedTransactions.size should be(0)
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
      snapshotManager.committedTransactions.size should be(0)
      // check knownVersionVectors
      snapshotManager.getKnownVectorClocks.size should be(0)
      // check globalStableSnapshot
      snapshotManager.globalStableSnapshot._1.compareTo(vv0) should be(VersionVector.Same)
      snapshotManager.globalStableSnapshot._2.size should be(0)

      // commit
      val commitVv = snapshotManager.commit(tid)
      // check currentTransactions
      snapshotManager.currentTransactions.contains(tid) should be(false)
      // check committedTransactions
      snapshotManager.committedTransactions.size should be(1)
      snapshotManager.committedTransactions.contains(commitVv) should be(true)
      snapshotManager.committedTransactions(commitVv)(key1).data should be(c1)
      snapshotManager.committedTransactions(commitVv)(key2).data should be(c2)
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
      commitVv.compareTo(OneVersionVector(node1, 1)) should be(VersionVector.Same)

      // check currentTransactions
      snapshotManager.currentTransactions.contains(tid) should be(false)
      // check committedTransactions
      snapshotManager.committedTransactions.size should be(1)
      snapshotManager.committedTransactions.contains(commitVv) should be(true)
      snapshotManager.committedTransactions(commitVv)(key1).data should be(c1)
      snapshotManager.committedTransactions(commitVv)(key2).data should be(c2)
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
    }

    "update GSS correctly" in {
      // empty KnownVectorClocks
      snapshotManager.getKnownVectorClocks.size should be(0)
      snapshotManager.globalStableSnapshot._1.compareTo(OneVersionVector(selfUniqueAddress.uniqueAddress, 0)) should be(VersionVector.Same)

      snapshotManager.updateKnownVersionVectors(node1, vv1)
      snapshotManager.getKnownVectorClocks should be(Map(node1 -> vv1))
      snapshotManager.globalStableSnapshot._1.compareTo(vv1) should be(VersionVector.Same)

      snapshotManager.updateKnownVersionVectors(node2, vv4)
      snapshotManager.getKnownVectorClocks should be(Map(node1 -> vv1, node2 -> vv4))
      snapshotManager.globalStableSnapshot._1.compareTo(vv1) should be(VersionVector.Same)

      snapshotManager.updateKnownVersionVectors(node1, vv4)
      snapshotManager.getKnownVectorClocks should be(Map(node1 -> vv4, node2 -> vv4))
      snapshotManager.globalStableSnapshot._1.compareTo(vv4) should be(VersionVector.Same)

      snapshotManager.updateKnownVersionVectors(node3, vv5)
      snapshotManager.getKnownVectorClocks should be(Map(node1 -> vv4, node2 -> vv4, node3 -> vv5))
      snapshotManager.globalStableSnapshot._1.compareTo(vv4) should be(VersionVector.Same)

      snapshotManager.updateKnownVersionVectors(node2, vv2)
      snapshotManager.getKnownVectorClocks should be(Map(node1 -> vv4, node2 -> vv2, node3 -> vv5))
      snapshotManager.globalStableSnapshot._1.compareTo(vv4) should be(VersionVector.Same)

      snapshotManager.updateKnownVersionVectors(node1, vv2)
      snapshotManager.getKnownVectorClocks should be(Map(node1 -> vv2, node2 -> vv2, node3 -> vv5))
      snapshotManager.globalStableSnapshot._1.compareTo(vv2) should be(VersionVector.Same)

      snapshotManager.updateKnownVersionVectors(node1, vv3)
      snapshotManager.getKnownVectorClocks should be(Map(node1 -> vv3, node2 -> vv2, node3 -> vv5))
      snapshotManager.globalStableSnapshot._1.compareTo(vv2) should be(VersionVector.Same)

      snapshotManager.updateKnownVersionVectors(node2, vv3)
      snapshotManager.getKnownVectorClocks should be(Map(node1 -> vv3, node2 -> vv3, node3 -> vv5))
      snapshotManager.globalStableSnapshot._1.compareTo(vv3) should be(VersionVector.Same)
    }

  }

}