
package com.udgrp.conf

import org.apache.spark.sql.SparkSession

/**
  * Created by kejw on 2017/5/15.
  */
object SparkInit {

  def initSpark: SparkSession = {
    val appName = Constants.APPNAME
    val master = Constants.MASTER
    val timeout = Constants.TIMEOUT
    val heartbeatInterval = Constants.HEARTBEATINTERVAL
    val spark_serializer = Constants.SPARK_SERIALIZER
    val spark_executor_extrajavaoptions = Constants.SPARK_EXECUTOR_EXTRAJAVAOPTIONS

    val sparkSession = SparkSession.builder
      .master(master)
      .appName(appName)
      .config("spark.network.timeout", timeout)
      .config("spark.executor.heartbeatInterval", heartbeatInterval)
      .config("spark.serializer", spark_serializer)
      .config("spark.executor.extraJavaOptions", spark_executor_extrajavaoptions)
      .getOrCreate()
    sparkSession
  }
}
