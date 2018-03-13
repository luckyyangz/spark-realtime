
package com.udgrp.service

import com.udgrp.common.SparkDriver
import com.udgrp.conf.Constants
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.OffsetRange

/**
  * Created by kejw on 2017/10/12.
  */
object Main extends SparkDriver {

  def main(args: Array[String]) {
    Main.run(Constants.TOPICS)
  }

  /**
    * 加载静态数据
    */
  override def loadStaticData(ss: SparkSession): Unit = {
    println("loadStaticData:加载静态数据")
    //TODO
  }

  /**
    * 子类根据自身需求重写该方法
    */
  override def processDStream(dStream: InputDStream[ConsumerRecord[String, String]]): Unit = {
    println("processDStream:子类根据自身需求重写该方法")
    dStream.foreachRDD(rdd => {
      rdd.foreachPartition(f => {
        f.foreach(p => {
          if (p.topic() == Constants.KAFKA_GPS_TOPIC) {
            println(p.topic() + " value==:" + p.value())
            //updateOffSets(rdd,p.topic)
          } else if (p.topic() == Constants.KAFKA_GPS_MISS_TOPIC) {
            println(p.topic() + " value==:" + p.value())
            //updateOffSets(rdd,p.topic)
          }else if (p.topic() == Constants.KAFKA_GPS_TOPIC) {
            println(p.topic() + " value==:" + p.value())
            //updateOffSets(rdd,p.topic)
          }
        })
      })
    })
  }
}


















