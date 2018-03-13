
package com.udgrp.common

import java.util.concurrent.Executors

import com.udgrp.conf.{Constants, SparkInit}
import com.udgrp.zktools.{ZKOffsetsStore, ZKStringSerializer}
import kafka.utils.ZkUtils
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.Map

/**
  * Created by kejw on 2017/5/15.
  */
trait SparkDriver extends ConnPools {
  val nThreads = Runtime.getRuntime().availableProcessors()
  val excutorpools = Executors.newCachedThreadPool
  def run(topics:Array[String]): Unit = {
    driverRunner(topics)
  }

  private def driverRunner(topics:Array[String]): Unit = {
    val sparkSession = SparkInit.initSpark
    logger.info("spark config inited")
    loadStaticData(sparkSession)
    val streamingContext = initSparkStreaming(sparkSession.sparkContext,topics)
    streamingContext.start()
    logger.info("streamingContext start successfully")
    streamingContext.awaitTermination()
  }

  private def initSparkStreaming(sc: SparkContext,topics:Array[String]): StreamingContext = {
    val streamingContext = new StreamingContext(sc, Seconds(Constants.BATCH_DURATION))
    streamingContext.checkpoint(Constants.SPARK_CHECKPOINT)
    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, getKafkaParms, readOffsets(topics))
    )
    processDStream(dStream)
    streamingContext
  }

  private def getKafkaParms: Map[String, Object] = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Constants.KAFKA_SERVER,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> Constants.CONSUMER_GROUP_ID,
      "auto.offset.reset" -> Constants.OFFSET_RESET,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    kafkaParams
  }

  /**
    * 读取偏移量
    *
    * @param topics
    */
  def readOffsets(topics: Array[String]): mutable.Map[TopicPartition, Long] = {
    val zkUtils = getZKConn
    val offsetsStore = new ZKOffsetsStore(Constants.ZK_CLUSTER)
    val offsetsMap = offsetsStore.readOffsets(topics, Constants.CONSUMER_GROUP_ID,zkUtils)
    returnZKConn(zkUtils)
    offsetsMap
  }

  /**
    * 保存偏移量
    *
    * @param rdd
    */
  def updateOffSets(rdd: RDD[ConsumerRecord[String, String]],topic:String): Unit = {
    val zkUtils = getZKConn
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    val offsetsStore = new ZKOffsetsStore(Constants.ZK_CLUSTER)
    offsetsStore.saveOffsets(topic,Constants.CONSUMER_GROUP_ID, offsetRanges, true,zkUtils)
    returnZKConn(zkUtils)
  }

  /**
    * 加载静态数据
    */
  def loadStaticData(ss: SparkSession)

  /**
    * 子类根据自身需求重写该方法
    */
  def processDStream(dStream: InputDStream[ConsumerRecord[String, String]])
}
