/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.cluster.UniqueAddress
import akka.cluster.ddata.Key.KeyId
import akka.cluster.ddata.Replicator.Internal.DataEnvelope
import akka.cluster.ddata.SnapshotManager.{ DataEntries, Snapshot }
import org.slf4j.{ Logger, LoggerFactory }

import scala.collection.mutable

/**
 * INTERNAL API: Used by the Replicator actor.
 */
object SnapshotManager {
  type DataEntries = Map[KeyId, DataEnvelope]
  type Snapshot = (VersionVector, DataEntries)

  val log: Logger = LoggerFactory.getLogger("akka.cluster.ddata.SnapshotManager")

  def apply(selfUniqueAddress: UniqueAddress): SnapshotManager = {
    new SnapshotManager(
      selfUniqueAddress,
      mutable.Map.empty,
      (VersionVector(selfUniqueAddress, 0), Map.empty),
//      (VersionVector.empty, Map.empty),
      mutable.TreeMap.empty[VersionVector, DataEntries](VersionVectorOrdering),
      mutable.HashMap.empty[Transaction.TransactionId, (Snapshot, Boolean)])
  }
}

private[akka] class SnapshotManager(
    val selfUniqueAddress: UniqueAddress,
    private val knownVersionVectors: mutable.Map[UniqueAddress, VersionVector],
    var globalStableSnapshot: Snapshot,
    //    var lastestLocalSnapshot: Snapshot,
    var localSnapshots: mutable.TreeMap[VersionVector, DataEntries],
    // Boolean used to check if read only transaction: increment or not vv
    val currentTransactions: mutable.HashMap[Transaction.TransactionId, (Snapshot, Boolean)]) {
  import SnapshotManager._

  def transactionPrepare(tid: Transaction.TransactionId): Snapshot = {
    log.debug("SnapshotManager::transactionPrepare(tid=[{}])", tid)
    val res = (latestStableSnapshotVersionVector, Map.empty[KeyId, DataEnvelope])
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
    val newGssData = mutable.Map.empty[KeyId, DataEnvelope]
    newGssData ++= globalStableSnapshot._2  // apply old GSS values

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
  def get(tid: Transaction.TransactionId, key: KeyId): Option[ReplicatedData] = {
    log.debug("SnapshotManager::get(tid=[{}], key=[{}])", tid, key)

    // apply currentTransactions
    currentTransactions.get(tid) match {
      case Some(currentTransactionSnapshot) =>
        // materialize data for given key
        val newData = mutable.Map.empty[KeyId, DataEnvelope] // TODO: I don't need a map for just 1 value

        // apply globalStableSnapshot
        newData ++= globalStableSnapshot._2.filter(x => x._1 == key)

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

        newData.get(key) match {
          case Some(d) => Some(d.data)
          case None    => None
        }
      case None => None
    }

  }

  def update(tid: Transaction.TransactionId, updatedData: Map[KeyId, DataEnvelope]): Unit = {
    log.debug("SnapshotManager::update(tid=[{}], updatedData=[{}])", tid, updatedData)
    updatedData.foreach(p => update(tid, p._1, p._2))
  }

  def update(tid: Transaction.TransactionId, key: KeyId, envelope: DataEnvelope): Unit = {
    log.debug("SnapshotManager::update(tid=[{}], key=[{}], envelope=[{}])", tid, key, envelope)
    currentTransactions.get(tid) match {
      case Some(d) =>
        val newData = d._1._2.updated(key, envelope)
        currentTransactions.update(tid, ((d._1._1, newData), true))
      case None => // transaction prepare has not been called
    }
  }

  def updateFromGossip(version: VersionVector, updatedData: Map[KeyId, DataEnvelope]): Unit = {
    log.debug("SnapshotManager::updateFromGossip(version=[{}], updatedData=[{}])", version, updatedData)
//    updatedData.foreach(p => updateFromGossip(version, p._1, p._2))

    def last: Snapshot = localSnapshots.last

    val commitVv = last._1.merge(version)
    assert(commitVv.compareTo(last._1) == VersionVector.After)

    // materialize before current snapshot
    val newData = mutable.Map.empty[KeyId, DataEnvelope]

    localSnapshots.foreach(p => {
      def localSnapshotData = p._2

      updatedData.keys.foreach(updatedDataKey => {
        localSnapshotData.get(updatedDataKey) match {
          // localSnapshot has a value for key
          case Some(localSnapshotValue) =>
            newData.get(updatedDataKey) match {
              case Some(newDataValue) => newData.update(updatedDataKey, newDataValue.merge(localSnapshotValue))
              case None               => newData.update(updatedDataKey, localSnapshotValue)
            }

          case None =>
        }
      })
    })

    assert(newData.keySet == updatedData.keySet)
    // apply currentTransactionSnapshot
    updatedData.foreach(kv => {
      newData.update(kv._1, newData(kv._1).merge(kv._2))
    })

    localSnapshots.addOne((commitVv, newData.toMap))
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

        // materialize before current snapshot
        val newData = mutable.Map.empty[KeyId, DataEnvelope]

        // apply globalStableSnapshot
        newData ++= globalStableSnapshot._2.filter(x => currentTransactionSnapshot._2.keySet.contains(x._1))

        // apply localSnapshots
        localSnapshots.foreach(p => {
          def localSnapshotData = p._2

          currentTransactionSnapshot._2.keys.foreach(currentTransactionSnapshotKey => {
            localSnapshotData.get(currentTransactionSnapshotKey) match {
              // localSnapshot has a value for key
              case Some(localSnapshotValue) =>
                newData.get(currentTransactionSnapshotKey) match {
                  case Some(newDataValue) =>
                    newData.update(currentTransactionSnapshotKey, newDataValue.merge(localSnapshotValue))
                  case None => newData.update(currentTransactionSnapshotKey, localSnapshotValue)
                }

              case None =>
            }
          })
        })

//        assert(newData.keySet == currentTransactionSnapshot._2.keySet)
        // apply currentTransactionSnapshot
        currentTransactionSnapshot._2.foreach(kv => {
          newData.get(kv._1) match {
            case Some(d) => newData.update(kv._1, d.merge(kv._2))
            case None    => newData.update(kv._1, kv._2)
          }
        })

        // update localSnapshots
        if (newData.nonEmpty) localSnapshots.update(commitVv, newData.toMap)
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
