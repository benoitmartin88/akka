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

  def apply(selfUniqueAddress: UniqueAddress): SnapshotManager = {
    currentVersionVector = VersionVector(selfUniqueAddress, 0)
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

  def increment(node: UniqueAddress): Unit = {
    currentVersionVector = currentVersionVector.increment(node)
  }

  def getCurrentVersionVector(): VersionVector = currentVersionVector

  def mergeCurrentVersion(that: VersionVector): VersionVector = {
    currentVersionVector = currentVersionVector.merge(that)
    currentVersionVector
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

    val version =
      if (envelope.version < currentVersionVector) envelope.version // old snapshot
      else mergeCurrentVersion(envelope.version)

    snapshots.get(version) match {
      case Some(foundDataEntries) => snapshots.addOne((version, foundDataEntries.updated(key, (envelope, DeletedDigest))))
      case None =>
        val newEnvelope = envelope.copy(version = VersionVector.empty) // remove version vector because information is redundant
        val newDataEntries = dataEntries.updated(key, (newEnvelope, DeletedDigest)) // TODO: recalculate digest ?
        snapshots.addOne((version, newDataEntries))
    }
  }

  def update(dataEntries: DataEntries, kvs: immutable.Iterable[(KeyId, DataEnvelope)]): Unit = {
    kvs.foreach(k => update(dataEntries, k._1, k._2))
  }

}
