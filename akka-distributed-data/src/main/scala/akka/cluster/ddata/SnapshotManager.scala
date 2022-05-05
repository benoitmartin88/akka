/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.cluster.UniqueAddress
import akka.cluster.ddata.Key.KeyId
import akka.cluster.ddata.Replicator.Internal.{DataEnvelope, DeletedDigest, Digest}
import akka.cluster.ddata.SnapshotManager.DataEntries
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.{immutable, mutable}

/**
 * INTERNAL API: Used by the Replicator actor.
 */
object SnapshotManager {
  type DataEntries = Map[KeyId, (DataEnvelope, Digest)]
//  def newEmptyDataEntries: DataEntries = Map.empty[KeyId, (DataEnvelope, Digest)]

  val log: Logger = LoggerFactory.getLogger("akka.cluster.ddata.SnapshotManager")
  var currentVersionVector: VersionVector = VersionVector.empty
  var lastStableSnapshot: (VersionVector, DataEntries) = (VersionVector.empty, Map.empty)

  def apply(selfUniqueAddress: UniqueAddress): SnapshotManager = {
    currentVersionVector = VersionVector(selfUniqueAddress, 0)
    lastStableSnapshot = (VersionVector(selfUniqueAddress, 0), Map.empty)
    new SnapshotManager(selfUniqueAddress, mutable.TreeMap.empty[VersionVector, DataEntries](VersionVectorOrdering))
  }
}

/**
 * TODO: duplicate version vector saved: once as TreeMap key and in DataEnvelope.
 */
private[akka] class SnapshotManager(
    val selfUniqueAddress: UniqueAddress,
    val snapshots: mutable.TreeMap[VersionVector, DataEntries]) {
  import SnapshotManager._

  def currentVersionVector = SnapshotManager.currentVersionVector
  def lastStableSnapshot = SnapshotManager.lastStableSnapshot

  def increment(node: UniqueAddress): Unit = {
    SnapshotManager.currentVersionVector = currentVersionVector.increment(node)
  }

  def mergeCurrentVersion(that: VersionVector): VersionVector = {
    SnapshotManager.currentVersionVector = currentVersionVector.merge(that)
    currentVersionVector
  }

  def sumVersionVector(vv: VersionVector): Int = {
    var i: Int = 0
    var paddedVv = vv
    var paddedLastStableSnapshot = SnapshotManager.lastStableSnapshot
    // make sure they are the same size
    if (lastStableSnapshot._1.size > vv.size) {
      lastStableSnapshot._1.versionsIterator.foreach(p => {
        val node = p._1
        if (!vv.contains(node)) {
          paddedVv = vv.pad(node)
        }
      })
    } else if (lastStableSnapshot._1.size < vv.size) {
      vv.versionsIterator.foreach(p => {
        val node = p._1
        if (!lastStableSnapshot._1.contains(node)) {
          paddedLastStableSnapshot = (lastStableSnapshot._1.pad(node), lastStableSnapshot._2)
        }
      })
    }

    assert(paddedLastStableSnapshot._1.size == paddedVv.size)

    paddedLastStableSnapshot._1.versionsIterator
      .zip(paddedVv.versionsIterator)
      .foreach(x => {
        i += (x._1._2 - x._2._2).abs.toInt
      })

    i
  }

  /**
   * Returns the value associated to a given key with respect to a given version vector.
   * The version vector
   * @param version version vector to use as causal context
   * @param key key to lookup
   * @return value associated to the given key
   */
  def get(version: VersionVector, key: KeyId): Option[ReplicatedData] = {
    log.debug("SnapshotManager::get(version=[{}], key=[{}], ", version, key)
    snapshots.minAfter(version) match {
      case None => None
      case d =>
        if ((d.get._1 < version || d.get._1 == version) && d.get._2.contains(key)) Option(d.get._2(key)._1.data)
        else None
    }
  }

  /**
   * TODO: Here the full DataEntries map is copied !! This "trick" is only done to save a full snapshot and not have to implement a view materializer.
   * Does not increment vector clock
   */
  def update(dataEntries: DataEntries, key: KeyId, envelope: DataEnvelope): Unit = {
    log.debug("SnapshotManager::update(dataEntries=" + dataEntries + ", key=" + key + ", envelope=" + envelope + ")")

    def updateLastStableSnapshot(key: KeyId, envelope: DataEnvelope): Unit = {
      val version = envelope.version
      val newEnvelope = envelope.copy(version = VersionVector.empty) // remove version vector because information is redundant
      //      val newDataEntries = Map.empty.updated(key, (newEnvelope, DeletedDigest))
      val newDataEntries = lastStableSnapshot._2.updated(key, (newEnvelope, DeletedDigest))
      SnapshotManager.lastStableSnapshot = (version, newDataEntries)
    }

    val version = envelope.version

    // TODO: check this !
    if (sumVersionVector(version) > 1) {
      // not stable yet
      snapshots.get(version) match {
        case Some(foundDataEntries) =>
          snapshots.addOne((version, foundDataEntries.updated(key, (envelope, DeletedDigest))))
        case None =>
          val newEnvelope = envelope.copy(version = VersionVector.empty) // remove version vector because information is redundant
          val newDataEntries = Map.empty.updated(key, (newEnvelope, DeletedDigest))
          snapshots.addOne((version, newDataEntries))
      }
    } else {
      // stable: apply, than check if pending operations can be applied
      updateLastStableSnapshot(key, envelope)

      def findUpdateAndRemove(envelope: DataEnvelope): Unit = {
        snapshots.find(snapshot => sumVersionVector(snapshot._1) == 1) match {
          case Some(d) =>
            val version = d._1
            d._2.foreach(p => updateLastStableSnapshot(p._1, p._2._1.copy(version = version)))
            snapshots.remove(d._1)

            findUpdateAndRemove(envelope)

          case _ =>
        }
      }

      // check if other operations can be applied
      findUpdateAndRemove(envelope)
    }
  }

  def update(dataEntries: DataEntries, kvs: immutable.Iterable[(KeyId, DataEnvelope)]): Unit = {
    kvs.foreach(k => update(dataEntries, k._1, k._2))
  }

}
