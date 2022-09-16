/*
 * Copyright (C) 2021-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.cluster.UniqueAddress
import akka.cluster.ddata.Key.KeyR
import akka.cluster.ddata.Replicator.Internal.DataEnvelope
import akka.cluster.ddata.SnapshotManager.{DataEntries, Snapshot}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * INTERNAL API: Used by the Replicator actor.
 */
object SnapshotManager {
  type DataEntries = Map[KeyR, DataEnvelope]
  type Snapshot = (VersionVector, DataEntries)

  val log: Logger = LoggerFactory.getLogger("akka.cluster.ddata.SnapshotManager")

  def apply(selfUniqueAddress: UniqueAddress): SnapshotManager = {
    new SnapshotManager(
      selfUniqueAddress,
      mutable.Map.empty,
      (VersionVector(selfUniqueAddress, 0), Map.empty),
      mutable.TreeMap.empty[VersionVector, DataEntries](VersionVectorOrdering),
      mutable.HashMap.empty[Transaction.TransactionId, (Snapshot, Boolean)])
  }
}

private[akka] class SnapshotManager(
    val selfUniqueAddress: UniqueAddress,
    private val knownVersionVectors: mutable.Map[UniqueAddress, VersionVector],
    var globalStableSnapshot: Snapshot,
    var localSnapshots: mutable.TreeMap[VersionVector, DataEntries],
    // Boolean used to check if read only transaction: increment or not vv
    val currentTransactions: mutable.HashMap[Transaction.TransactionId, (Snapshot, Boolean)]) {
  import SnapshotManager._

  def transactionPrepare(tid: Transaction.TransactionId): Snapshot = {
    log.debug("SnapshotManager::transactionPrepare(tid=[{}])", tid)
    val res = (latestStableSnapshotVersionVector, Map.empty[KeyR, DataEnvelope])
    currentTransactions.update(tid, (res, false))
    res
  }

  /**
   * GSS + local committed operations
   * Used for transaction start
   * TODO: strip empty vv ?
   */
  private[akka] def latestStableSnapshot: Snapshot = {
    localSnapshots.lastOption match {
      case Some(last) =>
        val vv = globalStableSnapshot._1.merge(last._1)
        val data = globalStableSnapshot._2 ++ last._2
        (vv, data)
      case None => globalStableSnapshot
    }
  }

  private[akka] def latestStableSnapshotVersionVector: VersionVector = {
    localSnapshots.lastOption match {
      case Some(last) => globalStableSnapshot._1.merge(last._1)
      case None       => globalStableSnapshot._1
    }
  }

  def getKnownVectorClocks: Map[UniqueAddress, VersionVector] = knownVersionVectors.toMap

  def updateKnownVersionVectors(node: UniqueAddress, versionVector: VersionVector): Unit = {
    knownVersionVectors.update(node, versionVector)
    updateGlobalStableSnapshot()
    log.debug(
      "SnapshotManager::updateKnownVersionVectors(node=[{}], versionVector=[{}]): GSS=[{}]",
      node,
      versionVector,
      globalStableSnapshot)
  }

  def updateGlobalStableSnapshot(): Unit = {
    val vvs = knownVersionVectors.values.toList
    val newGssVv = vvs.size match {
      case 0 => VersionVector.empty
      case 1 => vvs.head
      case _ =>
        var res = VersionVector.empty

        val vv1 = vvs.head
        vv1.versionsIterator.foreach(p => {
          val node1 = p._1
          val node1Vv = p._2
//          println("> node1 = " + node1 + ", node1Vv = " + node1Vv)

          for (i <- 1 until vvs.size) {
            val vv2 = vvs(i)
            vv2.versionsIterator.foreach(p => {
              val node2 = p._1
              val node2Vv = p._2
//              println(">> node2 = " + node2 + ", node2Vv = " + node2Vv)

              if (node1 == node2) {
                if (res.contains(node1)) {
                  res = res.merge(VersionVector(node1, math.min(node2Vv, res.versionAt(node1))))
                } else {
                  res = res.merge(VersionVector(node1, math.min(node2Vv, node1Vv)))
                }
              }

//              println("res = " + res)
//              println()
            })
          }
        })
        res
    }

    // materialize before current snapshot
    val newGssData = mutable.Map.empty[KeyR, DataEnvelope]
    newGssData ++= globalStableSnapshot._2 // apply old GSS values

    // apply localSnapshots
    localSnapshots
      .rangeTo(newGssVv)
      .foreach(p => {
        def localSnapshotData = p._2

        localSnapshotData.foreach(p2 => {
          newGssData.get(p2._1) match {
            case Some(newDataValue) => newGssData.update(p2._1, newDataValue.merge(p2._2))
            case None               => newGssData.update(p2._1, p2._2)
          }
        })

        // clear localSnapshot that has been merged in GSS
        localSnapshots.remove(p._1)
      })

    if (newGssData.nonEmpty) globalStableSnapshot = (newGssVv, newGssData.toMap)
  }

  /**
   * Returns the value associated to a given key with respect to a given version vector.
   * @param key key to lookup
   * @return value associated to the given key
   */
  def get(tid: Transaction.TransactionId, key: KeyR): Option[ReplicatedData] = {
    log.debug("SnapshotManager::get(tid=[{}], key=[{}])", tid, key)

    // apply currentTransactions
    currentTransactions.get(tid) match {
      case Some(currentTransactionSnapshot) =>
        // materialize data for given key
        val newData = mutable.Map.empty[KeyR, DataEnvelope] // TODO: I don't need a map for just 1 value

        // apply globalStableSnapshot
        newData ++= globalStableSnapshot._2.filter(x => x._1 == key)
//        println("==== 1 newData=" + newData)

        // apply localSnapshots
        localSnapshots
          .rangeTo(currentTransactionSnapshot._1._1)
          .foreach(p => {
            def localSnapshotData = p._2

            localSnapshotData.get(key) match {
              // localSnapshot has a value for key
              case Some(localSnapshotValue) =>
                newData.get(key) match {
                  case Some(newDataValue) =>
                    newData.update(key, newDataValue.merge(localSnapshotValue))
                  case None => newData.update(key, localSnapshotValue)
                }

              case None =>
            }
          })

//        println("==== 2 newData=" + newData)
//        println("==== 3 currentTransactionSnapshot=" + currentTransactionSnapshot)

        // apply currentTransactionSnapshot
        currentTransactionSnapshot._1._2.foreach(d => {
//          println("==== 4 d=" + d)
          newData.get(d._1) match {
            case Some(newDataValue) =>
//              println("==== 5 merge key=" + key + ", newDataValue=" + newDataValue + ", d._2=" + d._2)
              newData.update(d._1, newDataValue.merge(d._2))
            case None =>
//              println("==== 5' merge key=" + key + ", d._2=" + d._2)
              newData.update(d._1, d._2)
          }
        })
//        println("==== 6 newData=" + newData)

        newData.get(key) match {
          case Some(d) => Some(d.data)
          case None    => None
        }
      case None => None
    }
  }

  def update(tid: Transaction.TransactionId, updatedData: DataEntries): Unit = {
    log.debug("SnapshotManager::update(tid=[{}], updatedData=[{}])", tid, updatedData)
    updatedData.foreach(p => update(tid, p._1, p._2))
  }

  def update(tid: Transaction.TransactionId, key: KeyR, envelope: DataEnvelope): Unit = {
    log.debug("SnapshotManager::update(tid=[{}], key=[{}], envelope=[{}])", tid, key, envelope)
    println("SnapshotManager::update(tid=[" + tid + "], key=[" + key + "]), envelope=" + envelope)
    currentTransactions.get(tid) match {
      case Some(d) =>
        val newData = d._1._2.updated(key, envelope)
        currentTransactions.update(tid, ((d._1._1, newData), true))
      case None => // transaction prepare has not been called
    }
  }

  def updateFromGossip(version: VersionVector, updatedData: DataEntries): Unit = {
    log.debug("SnapshotManager::updateFromGossip(version=[{}], updatedData=[{}])", version, updatedData)

    localSnapshots.get(version) match {
      case Some(localSnapshot) =>
        // a version exists, merge existing values

        updatedData.foreach(kv => {
          localSnapshot.get(kv._1) match {
            case Some(p) => localSnapshots.update(version, localSnapshot.updated(kv._1, p.merge(kv._2)))
            case None    => localSnapshots.update(version, localSnapshot.updated(kv._1, kv._2))
          }
        })

      case None => localSnapshots.update(version, updatedData)
    }
  }

  def commit(tid: Transaction.TransactionId): VersionVector = {
    log.debug("SnapshotManager::commit(tid=[{}])", tid)

    val res = currentTransactions.get(tid) match {
      case Some(currentTransaction) =>
        // found transaction
        def currentTransactionSnapshot: Snapshot = currentTransaction._1
        def increment: Boolean = currentTransaction._2
        def last: Snapshot = localSnapshots.lastOption match {
          case Some(l) => l
          case None    => globalStableSnapshot
        }

        val commitVv =
          if (increment) last._1.increment(selfUniqueAddress)
          else last._1

//        assert(commitVv.compareTo(last._1) == (VersionVector.After | VersionVector.Same))

        if (currentTransactionSnapshot._2.nonEmpty) localSnapshots.update(commitVv, currentTransactionSnapshot._2)
        commitVv
      case None => VersionVector.empty // transaction not found
    }

    currentTransactions.remove(tid)
    res
  }

  def abort(tid: Transaction.TransactionId): Unit = {
    log.debug("SnapshotManager::abort(tid=[{}])", tid)
    currentTransactions.remove(tid)
  }

}
