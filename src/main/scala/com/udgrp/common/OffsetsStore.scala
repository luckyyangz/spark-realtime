
package com.udgrp.common

import kafka.utils.ZkUtils
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import org.slf4j.LoggerFactory

import scala.collection.mutable._

/**
  * Created by yxl on 17/4/14.
  */
trait OffsetsStore extends Serializable {

  @transient val logger = LoggerFactory.getLogger(classOf[OffsetsStore])

  def readOffsets(topic: String, groupId: String): Option[Map[TopicPartition, Long]]

  def readOffsets(topics: Array[String], groupId: String,zkUtils: ZkUtils): Map[TopicPartition, Long]

  def saveOffsets(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit

  def saveOffsets(groupId: String, offsetRanges: Array[OffsetRange],storeEndOffset: Boolean,zkUtils: ZkUtils): Unit

  def saveOffsets(topic: String,groupId: String, offsetRanges: Array[OffsetRange],storeEndOffset: Boolean,zkUtils: ZkUtils): Unit

}
