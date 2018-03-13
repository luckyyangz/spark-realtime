
package com.udgrp.zktools

/**
  * Created by kejw on 2017/10/12.
  */

import com.udgrp.common.OffsetsStore
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010._

import scala.collection.mutable
import scala.collection.mutable._

class ZooKeeperOffsetsStore(zkHosts: String) extends OffsetsStore {

  private val zkClient = new ZkClient(zkHosts, 10000, 10000, new MyZKStringSerializer)

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

  override def readOffsets(topics: Array[String], groupId: String, zkUtils: ZkUtils): mutable.Map[TopicPartition, Long] = ???

  override def saveOffsets(groupId: String, offsetRanges: Array[OffsetRange], storeEndOffset: Boolean, zkUtils: ZkUtils): Unit = ???

  override def saveOffsets(topic: String, groupId: String, offsetRanges: Array[OffsetRange], storeEndOffset: Boolean, zkUtils: ZkUtils): Unit = ???
}