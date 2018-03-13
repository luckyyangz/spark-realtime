
package com.udgrp.conf

import com.typesafe.config.ConfigFactory

/**
  * Created by kejw on 2017/5/23.
  */
object Constants {

  /**
    * 测试环境
    */
  val conf = ConfigFactory.load("config.properties")
  /**
    * 生产环境
    */
  //val conf = ConfigFactory.parseFile(new File("C:/tmp/application.properties"))

  /**
    * SPARK配置参数
    */
  val APPNAME = conf.getString("appName")
  val MASTER = conf.getString("master")
  val TIMEOUT = conf.getString("spark.network.timeout")
  val HEARTBEATINTERVAL = conf.getString("spark.executor.heartbeatInterval")
  val SPARK_SERIALIZER = conf.getString("spark.serializer")
  val SPARK_EXECUTOR_EXTRAJAVAOPTIONS = conf.getString("spark.executor.extraJavaOptions")

  /**
    * POOLCONF配置参数
    */
  val MAXIDLE = conf.getInt("maxIdle")
  val MAXTOTAL = conf.getInt("maxTotal")
  val MAXWAITMILLIS = conf.getInt("maxWaitMillis")
  val TESTONBORROW = conf.getBoolean("testOnBorrow")
  val TESTONRETURN = conf.getBoolean("testOnReturn")
  val TESTONCREATE = conf.getBoolean("testOnCreate")

  /**
    * JDBC配置参数
    */
  val DRIVER = conf.getString("driver")
  val USERNAME = conf.getString("userName")
  val PASSWORD = conf.getString("password")
  val URL = conf.getString("url")

  /**
    * KAFKA配置参数
    */
  val KAFKA_SERVER = conf.getString("KAFKA_SERVER")

  val KAFKA_GPS_TOPIC = conf.getString("KAFKA_GPS_TOPIC")
  val KAFKA_SHW_TOPIC = conf.getString("KAFKA_SHW_TOPIC")
  val KAFKA_GPS_MISS_TOPIC = conf.getString("KAFKA_GPS_MISS_TOPIC")

  val ACKS = conf.getString("ACKS")
  val RETRIES = conf.getString("RETRIES")
  val BATCH_SIZE = conf.getString("BATCH_SIZE")
  val LINGER_MS = conf.getString("LINGER_MS")
  val RECONNECT_BACKOFF_MS = conf.getString("RECONNECT_BACKOFF_MS")
  val MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = conf.getString("MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION")
  val RETRY_BACKOFF_MS = conf.getString("RETRY_BACKOFF_MS")
  val BUFFER_MEMORY = conf.getString("BUFFER_MEMORY")
  val COMPRESSION_TYPE = conf.getString("COMPRESSION_TYPE")
  val KEY_SERIALIZER = conf.getString("KEY_SERIALIZER")
  val VALUE_SERIALIZER = conf.getString("VALUE_SERIALIZER")
  val PARTITION_CLASS = conf.getString("PARTITION_CLASS")

  val CONSUMER_GROUP_ID = conf.getString("CONSUMER_GROUP_ID")

  /**
    * 程序输入参数
    */
  val EXLISTDATA = conf.getString("exlistdata")
  val GPSDATA = conf.getString("gpsdata")
  val GPS_DURATION = conf.getDouble("gps_duration")
  val STASPEED = conf.getString("staSpeed")

  /**
    * sql语句
    */
  val LOADGFCAR_SQL = conf.getString("loadGFCar_sql")
  val LOADWN_SQL = conf.getString("loadWN_sql")
  val LOADSTATIONDATA_SQL = conf.getString("loadStationData_sql")
  val LOADMIDSTATION_SQL = conf.getString("loadMidStation_sql")

  /**
    * spark streaming配置
    */
  val BATCH_DURATION = conf.getLong("batch_duration")
  val SPARK_CHECKPOINT = conf.getString("spark_checkpoint")
  val SAVE_HDFS = conf.getString("save_hdfs")
  val OFFSET_STORE = conf.getBoolean("OFFSET_STORE")
  val OFFSET_RESET = conf.getString("OFFSET_RESET")

  /**
    * kafka主题
    */
 // val TOPICS = Array(KAFKA_GPS_TOPIC,KAFKA_SHW_TOPIC,KAFKA_GPS_MISS_TOPIC)
  val TOPICS = Array(KAFKA_GPS_TOPIC,KAFKA_SHW_TOPIC)

  /**
    *zookeeper配置
    */
  val ZK_CLUSTER = conf.getString("zk_cluster")
}
