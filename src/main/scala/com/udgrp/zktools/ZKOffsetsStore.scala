
package com.udgrp.zktools

/**
  * Created by kejw on 2017/10/12.
  */

import com.udgrp.common.OffsetsStore
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010._
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.ACL

import scala.collection.mutable._
import scala.collection.{JavaConversions, mutable}

class ZKOffsetsStore(zkHosts: String) extends OffsetsStore {

  private val zkClient = new ZkClient(zkHosts, 10000, 10000, ZKStringSerializer)

  override def readOffsets(topic: String, consumer: String): Option[Map[TopicPartition, Long]] = {
    try {
      val zkUtils = ZkUtils(zkClient, false)
      //val topicPath = File.separator + topic + File.separator + consumer
      val topicPath = "/" + topic + "/" + consumer
      val (offsetsRangesStrOpt, _) = zkUtils.readDataMaybeNull(topicPath)
      zkUtils.close()
      offsetsRangesStrOpt match {
        case Some(offsetsRangesStr) =>
          val offsets = offsetsRangesStr.split(",")
            .map(s => s.split(":"))
            .map { case Array(partitionStr, offsetStr) => (new TopicPartition(topic, partitionStr.toInt) -> offsetStr.toLong) }
            .toMap
          Some(Map() ++ offsets)
        case None =>
          None
      }
    } catch {
      case ex: Exception => {
        logger.error(ex.getMessage)
        None
      }
    }
  }

  override def saveOffsets(topic: String, consumer: String, offsetRanges: Array[OffsetRange]): Unit = {
    try {
      val zkUtils = ZkUtils(zkClient, false)
      //val topicPath = File.separator + topic + File.separator + consumer
      val topicPath = "/" + topic + "/" + consumer
      val offsetsRangesStr = offsetRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.untilOffset}").mkString(",")
      logger.info(s"Writing offsets to ZooKeeper:$topic $consumer ${offsetsRangesStr}")
      zkUtils.updatePersistentPath(topicPath, offsetsRangesStr)
      zkUtils.close()
    } catch {
      case ex: Exception => {
        logger.error(ex.getMessage)
      }
    }
  }

  override def readOffsets(topics: Array[String], groupId: String, zkUtils: ZkUtils): mutable.Map[TopicPartition, Long] = {
    val topicPartOffsetMap = collection.mutable.HashMap.empty[TopicPartition, Long]
    val partitionMap = zkUtils.getPartitionsForTopics(topics)
    // /consumers/<groupId>/offsets/<topic>/
    partitionMap.foreach(topicPartitions => {
      val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, topicPartitions._1)
      topicPartitions._2.foreach(partition => {
        val offsetPath = zkGroupTopicDirs.consumerOffsetDir + "/" + partition
        try {
          val offsetStatTuple = zkUtils.readData(offsetPath)

          if (offsetStatTuple != null) {
            logger.info("retrieving offset details - topic: {}, partition: {}, offset: {}, node path: {}", Seq[AnyRef](topicPartitions._1, partition.toString, offsetStatTuple._1, offsetPath): _*)
            print("readOffsets:")
            println(topicPartitions._1, partition.toString, offsetStatTuple._1, offsetPath)
            topicPartOffsetMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)), offsetStatTuple._1.toLong)
          }
        } catch {
          case e: Exception =>
            logger.warn("retrieving offset details - no previous node exists:" + " {}, topic: {}, partition: {}, node path: {}", Seq[AnyRef](e.getMessage, topicPartitions._1, partition.toString, offsetPath): _*)
            topicPartOffsetMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)), 0L)
            print("readOffsets Exception:"+e.getMessage)
        }
      })
    })
    println("topicPartOffsetMap:" + topicPartOffsetMap)
    topicPartOffsetMap
  }

  override def saveOffsets(groupId: String, offsetRanges: Array[OffsetRange], storeEndOffset: Boolean, zkUtils: ZkUtils): Unit = {
    offsetRanges.foreach(or => {
      val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, or.topic)
      val acls = new ListBuffer[ACL]()
      val acl = new ACL
      acl.setId(ZooDefs.Ids.ANYONE_ID_UNSAFE)
      acl.setPerms(ZooDefs.Perms.ALL)
      acls += acl

      val offsetPath = zkGroupTopicDirs.consumerOffsetDir + "/" + or.partition
      val offsetVal = if (storeEndOffset) or.untilOffset else or.fromOffset
      zkUtils.updatePersistentPath(zkGroupTopicDirs.consumerOffsetDir + "/" + or.partition, offsetVal + "", JavaConversions.bufferAsJavaList(acls))
      print("saveOffsets:")
      println(or.topic, or.partition.toString, offsetVal.toString, offsetPath)
      logger.debug("persisting offset details - topic: {}, partition: {}, offset: {}, node path: {}", Seq[AnyRef](or.topic, or.partition.toString, offsetVal.toString, offsetPath): _*)
    })
  }

  override def saveOffsets(topic: String, groupId: String, offsetRanges: Array[OffsetRange], storeEndOffset: Boolean, zkUtils: ZkUtils): Unit = {
    offsetRanges.foreach(or => {
      if (topic == or.topic) {
        val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, topic)
        val acls = new ListBuffer[ACL]()
        val acl = new ACL
        acl.setId(ZooDefs.Ids.ANYONE_ID_UNSAFE)
        acl.setPerms(ZooDefs.Perms.ALL)
        acls += acl

        val offsetPath = zkGroupTopicDirs.consumerOffsetDir + "/" + or.partition
        val offsetVal = if (storeEndOffset) or.untilOffset else or.fromOffset
        zkUtils.updatePersistentPath(zkGroupTopicDirs.consumerOffsetDir + "/" + or.partition, offsetVal + "", JavaConversions.bufferAsJavaList(acls))
        print("saveOffsets:")
        println(or.topic, or.partition.toString, offsetVal.toString, offsetPath)
        logger.debug("persisting offset details - topic: {}, partition: {}, offset: {}, node path: {}", Seq[AnyRef](or.topic, or.partition.toString, offsetVal.toString, offsetPath): _*)
      }
    })
  }
}