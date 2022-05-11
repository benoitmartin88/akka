/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.cluster.ddata

import akka.cluster.UniqueAddress
import akka.cluster.ddata.Key.KeyId
import akka.cluster.ddata.Replicator.Internal.DataEnvelope
import akka.cluster.ddata.SnapshotManager.{DataEntries, Snapshot}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * INTERNAL API: Used by the Replicator actor.
 */
object SnapshotManager {
  type DataEntries = Map[KeyId, DataEnvelope]
  type Snapshot = (VersionVector, DataEntries)

  val log: Logger = LoggerFactory.getLogger("akka.cluster.ddata.SnapshotManager")
  var globalStableSnapshot: Snapshot = (VersionVector.empty, Map.empty)

  def apply(selfUniqueAddress: UniqueAddress): SnapshotManager = {
    globalStableSnapshot = (VersionVector(selfUniqueAddress, 0), Map.empty)
    new SnapshotManager(
      selfUniqueAddress,
      mutable.Map.empty,
      mutable.TreeMap.empty[VersionVector, DataEntries](VersionVectorOrdering),
      mutable.HashMap.empty[Transaction.TransactionId, (Snapshot, Boolean)])
  }
}

//private[akka] class Entry(
//   val snapshot: DataEntries,
//   val count: Int,
//   val incremented: Boolean) {}

private[akka] class SnapshotManager(
    val selfUniqueAddress: UniqueAddress,
    private val knownVersionVectors: mutable.Map[UniqueAddress, VersionVector],
    val committedTransactions: mutable.TreeMap[VersionVector, DataEntries],   // TODO: might not need a tree. maybe just a Snapshot is needed
    val currentTransactions: mutable.HashMap[Transaction.TransactionId, (Snapshot, Boolean)]) {
  import SnapshotManager._

  def globalStableSnapshot: Snapshot = SnapshotManager.globalStableSnapshot

  def transactionPrepare(tid: Transaction.TransactionId): Snapshot = {
    println("SnapshotManager::transactionPrepare() tid=" + tid)
    val res = latestStableSnapshot
    currentTransactions.update(tid, (res, false))
    res
  }

  /**
   * GSS + local committed operations
   * Used for transaction start
   * TODO: strip empty vv ?
   */
  private def latestStableSnapshot: Snapshot = {
    val lastCommitted = committedTransactions.lastOption
    lastCommitted match {
      case Some(last) =>
        val vv = globalStableSnapshot._1.merge(last._1)
        val data = globalStableSnapshot._2 ++ last._2

        println("!!!!!! latestStableSnapshot= " + (vv, data))
        (vv, data)
      case None =>
        println("!!!!!! latestStableSnapshot= " + globalStableSnapshot)
        globalStableSnapshot
    }
  }

  def getKnownVectorClocks: Map[UniqueAddress, VersionVector] = knownVersionVectors.toMap

  def updateKnownVersionVectors(node: UniqueAddress, versionVector: VersionVector): Unit = {
    knownVersionVectors.update(node, versionVector)
    updateGlobalStableSnapshot()

    println(">>>>>>>> GSS=" + globalStableSnapshot + ", knownVectorClocks=" + knownVersionVectors)
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

    val newData = committedTransactions.minAfter(newGssVv) match {
      case None => None
      case d    => d.get._2
//        if (d.get._1 < newGssVv || d.get._1 == newGssVv) Option(d.get._2(key)._1.data)
//        else None
    }

    SnapshotManager.globalStableSnapshot = (newGssVv, SnapshotManager.globalStableSnapshot._2 ++ newData)
  }

  /**
   * Returns the value associated to a given key with respect to a given version vector.
   * @param version version vector to use as causal context
   * @param key key to lookup
   * @return value associated to the given key
   */
  def get(tid: Transaction.TransactionId, key: KeyId): Option[ReplicatedData] = {
    log.debug("SnapshotManager::get(tid=[{}], key=[{}], ", tid, key)

    currentTransactions.get(tid) match {
      case Some(snapshot) =>
        snapshot._1._2.get(key) match {
          case Some(dd) => Some(dd.data)
          case None     => None
        }
      case None => None
    }
  }

  def update(tid: Transaction.TransactionId, updatedData: Map[KeyId, DataEnvelope]): Unit = {
    println("<<<<<<<< update() tid=" + tid + ", updatedData=" + updatedData)
    updatedData.foreach(p => update(tid, p._1, p._2))
  }

  def update(tid: Transaction.TransactionId, key: KeyId, envelope: DataEnvelope): Unit = {
    println("SnapshotManager::update() tid=" + tid + ", key=" + key + ", value=" + envelope.data)

    currentTransactions.get(tid) match {
      case Some(d) =>
        val newData = d._1._2.updated(key, envelope)
        currentTransactions.update(tid, ((d._1._1, newData), true))
      case None => // transaction prepare has not been called
    }
  }

  def updateFromGossip(version: VersionVector, updatedData: Map[KeyId, DataEnvelope]): Unit = {
    println("<<<<<<<< updateFromGossip() version=" + version + ", updatedData=" + updatedData)
    updatedData.foreach(p => updateFromGossip(version, p._1, p._2))
  }

  /**
   * Does not increment vector clock
   */
  private def updateFromGossip(version: VersionVector, key: KeyId, envelope: DataEnvelope): Unit = {
    println("SnapshotManager::updateFromGossip(key=" + key + ", envelope=" + envelope + ")")

    val lastCommitted = committedTransactions.lastOption

    lastCommitted match {
      case Some(last) =>
        // check if key is found in last committed
        val (vv, data) = last._2.get(key) match {
          case Some(d) =>
            // found key, check if data was concurrently modified
            version.compareTo(last._1) match {
              case VersionVector.Concurrent | VersionVector.After =>
                // received concurrent or more recent data: merge VV + data
                val vv = version.merge(last._1)
                val dd = envelope.merge(d.data.asInstanceOf[envelope.data.T])
                (vv, last._2.updated(key, dd))
              case _ =>
                assert(false) // TODO: other cases ? what to do ?
                (version, last._2.updated(key, envelope))
            }
          case None => (version, last._2.updated(key, envelope))
        }
        committedTransactions.update(vv, data)
      case None =>
    }
  }

  def commit(tid: Transaction.TransactionId): VersionVector = {
    log.debug("SnapshotManager::commit(tid=" + tid + ")")

    val res = currentTransactions.get(tid) match {
      case Some(snapshot) =>
        def increment = snapshot._2

        val lastCommitted = committedTransactions.lastOption
        val (newVV, newData) = lastCommitted match {
          case Some(last) =>
            snapshot._1._1.compareTo(last._1) match {
              case VersionVector.Concurrent =>
                // concurrent
                val vv =
                  if (increment) snapshot._1._1.merge(last._1).increment(selfUniqueAddress)
                  else snapshot._1._1.merge(last._1)

                // merge data
                var dd = last._2
                last._2.foreach(l => {
                  snapshot._1._2.foreach(s => {
                    if (s._1 == l._1) {
                      dd = dd.updated(s._1, s._2.merge(l._2))
                    } else {
                      dd = dd.updated(s._1, s._2)
                    }
                  })
                })

                (vv, dd)
              case _ =>
                // not concurrent
                val vv =
                  if (increment) snapshot._1._1.merge(last._1).increment(selfUniqueAddress)
                  else snapshot._1._1.merge(last._1)

                (vv, snapshot._1._2)
            }

          case None =>
            // there is no last committed : use snapshot
            val vv =
              if (increment) snapshot._1._1.increment(selfUniqueAddress)
              else snapshot._1._1

            (vv, snapshot._1._2)
        }

        committedTransactions.addOne((newVV, newData))
        newVV
      case None => VersionVector.empty
    }

    currentTransactions.remove(tid)
    res
  }

  def abort(tid: Transaction.TransactionId): Unit = {
    currentTransactions.remove(tid)
  }

}
