/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.cluster.ddata.Key.KeyId
import akka.cluster.ddata.Replicator.Internal.DataEnvelope
import akka.cluster.ddata.SnapshotManager.DataEntries

import scala.collection.mutable

/**
 * INTERNAL API: Used by the Replicator actor.
 */

object SnapshotManager {
  //  private type DataEntry = (DataEnvelope, Digest)
  private type DataEntries = Map[KeyId, DataEnvelope]

//  private val emptySnapshots = mutable.TreeMap.empty[VersionVector, DataEntries](VersionVectorOrdering)
//  val empty: SnapshotManager = SnapshotManager(emptySnapshots)

//  private val currentVersionVector: VersionVector = VersionVector.empty
//  private val snapshots = mutable.TreeMap.empty[VersionVector, DataEntries](VersionVectorOrdering)

  def apply(): SnapshotManager = new SnapshotManager(mutable.TreeMap.empty[VersionVector, DataEntries](VersionVectorOrdering))
}

private[akka] class SnapshotManager(val snapshots: mutable.TreeMap[VersionVector, DataEntries]) {
  import SnapshotManager._

  /**
   * Returns the value associated to a given key with respect to a given version vector.
   * The version vector
   * @param version version vector to use as causal context
   * @param key key to lookup
   * @return value associated to the given key
   */
  def get(version: VersionVector, key: KeyId): Option[ReplicatedData] = {
    snapshots.minAfter(version) match {
      case None => None
      case d =>
        if((d.get._1 < version || d.get._1 == version) && d.get._2.contains(key)) Option(d.get._2(key).data)
        else None
    }
  }

  def update(version: VersionVector, dataEntries: DataEntries, key: KeyId, envelope: DataEnvelope): Unit = {
    val newDataEntries = dataEntries.updated(key, envelope)
    snapshots.addOne((version, newDataEntries))
  }

}
