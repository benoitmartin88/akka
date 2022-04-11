/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.actor.Address
import akka.cluster.UniqueAddress
import akka.cluster.ddata.Key.KeyId
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
    type DataEntries = Map[KeyId, DataEnvelope]

    val dataEntries: DataEntries = TreeMap[KeyId, DataEnvelope]()
    val key = "key"
    val c1 = GCounter() :+ 1
    val c2 = GCounter() :+ 2
    val c3 = GCounter() :+ 3

    val vv0 = ManyVersionVector(TreeMap(node1 -> 0, node2 -> 0))
    val vv1 = ManyVersionVector(TreeMap(node1 -> 1, node2 -> 1))
    val vv2 = ManyVersionVector(TreeMap(node1 -> 2, node2 -> 2))
    val vv3 = ManyVersionVector(TreeMap(node1 -> 3, node2 -> 3))

    val snapshotManager = SnapshotManager()

    "update multiple times correctly" in {
      // same vv
      snapshotManager.update(vv2, dataEntries, key, DataEnvelope(c2))
      snapshotManager.snapshots.size should be(1)
      snapshotManager.snapshots.keySet.toList should be(List(vv2))

      snapshotManager.update(vv2, dataEntries, key, DataEnvelope(c2))
      snapshotManager.snapshots.size should be(1)
      snapshotManager.snapshots.keySet.toList should be(List(vv2))

      // bigger vv
      snapshotManager.update(vv3, dataEntries, key, DataEnvelope(c3))
      snapshotManager.snapshots.size should be(2)
      snapshotManager.snapshots.keySet.toList should be(List(vv2, vv3))

      // smaller vv
      snapshotManager.update(vv1, dataEntries, key, DataEnvelope(c1))
      snapshotManager.snapshots.keySet.toList should be(List(vv1, vv2, vv3))
    }

    "get previously added key with correct vector clock" in {
      snapshotManager.get(vv1, key).get should be(c1)
      snapshotManager.get(vv2, key).get should be(c2)
      snapshotManager.get(vv3, key).get should be(c3)
    }

    "get with an unknown vector clock" in {
      snapshotManager.get(vv0, key) should be(None)
    }

    "get unknown key" in {
      val key = "unknown key"
      snapshotManager.get(vv0, key) should be(None)
      snapshotManager.get(vv1, key) should be(None)
      snapshotManager.get(vv2, key) should be(None)
      snapshotManager.get(vv3, key) should be(None)
    }

  }

}
