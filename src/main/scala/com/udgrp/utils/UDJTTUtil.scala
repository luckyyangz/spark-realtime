
package com.udgrp.utils

import java.util.regex.Pattern

import org.apache.commons.lang.StringUtils
import org.apache.commons.lang3.time.FastDateFormat

import scala.collection.mutable.Set

/**
  * Created by gavin on 2017/3/7.
  */
object UDJTTUtil {
  val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val pattern = Pattern.compile("^[\u4e00-\u9fa5]{1}[a-zA-Z]{1}[a-zA-Z_0-9]{4}[a-zA-Z_0-9_\u4e00-\u9fa5]$")
  val pattern2 = Pattern.compile("^[\u4e00-\u9fa5]{2}[a-zA-Z]{1}[a-zA-Z_0-9]{4}[a-zA-Z_0-9_\u4e00-\u9fa5]$")

  /**
    * 过滤非法车牌
    *
    * @param car1
    * @param car2
    * @param car3
    * @param car4
    * @return
    */
  def getCarNum(car1: String, car2: String, car3: String, car4: String): String = {
    if (pattern.matcher(car3.replaceAll("\"", "")).matches()) {
      car4
    } else if (pattern.matcher(car4.replaceAll("\"", "")).matches()) {
      car3
    } else if (pattern.matcher(car1.replaceAll("\"", "")).matches()) {
      car1
    } else if (pattern.matcher(car2.replaceAll("\"", "")).matches()) {
      car2
    } else {
      ""
    }
  }

  /**
    * 筛选广东车牌
    *
    * @param car1
    * @param car2
    * @param car3
    * @param car4
    * @return
    */
  def getYUECarNum(car1: String, car2: String, car3: String, car4: String): String = {
    if (car4.replaceAll("\"", "").startsWith("粤")) {
      car4
    } else if (car3.replaceAll("\"", "").startsWith("粤")) {
      car3
    } else if (car1.replaceAll("\"", "").startsWith("粤")) {
      car1
    } else if (car2.replaceAll("\"", "").startsWith("粤")) {
      car2
    } else {
      ""
    }
  }

  /**
    * 针对上下高速时间的格式化
    *
    * @param time
    * @return
    */
  def formatTime(time: String): String = {
    val date = time.replaceAll("\"", "").substring(0, 10)
    val time_ = time.replaceAll("\"", "").substring(11, 19).replaceAll("\\.", ":")
    date + " " + time_
  }


  /**
    * 计算上下高速所耗时间
    */
  def getBetweenTimes(time1: String, time2: String): String = {
    if (StringUtils.isBlank(time1) || StringUtils.isBlank(time2)) {
      return "0" //没有进出口时间
    }
    val time = DateUtil.getTimeInMillis(time2) - DateUtil.getTimeInMillis(time1)
    if (time < 0) {
      return "0" //出口比进口时间大
    }
    //过滤掉那些诡异时间，如：1900...
    val betweenDays = time / (1000 * 24 * 60 * 60) //天
    if (betweenDays > 100) {
      return "0" //进口时间异常
    }
    val betweenSeconds = time / 1000 //秒
    f"$betweenSeconds%1.2f".toString
  }


  /**
    * 对于非白名单车辆，出入高速时间在2-5点之间的视为违规
    * 对于横跨2-5点的车辆，预先假设它中途停留了三个小时，
    * 再求速度，如果速度比预设速度大，则视为违规驾驶
    *
    * @param enTime        入口时间
    * @param exTime        出口时间
    * @param miles         行驶路程 / 米
    * @param standardSpeed 标准速度 / km/h
    * @param carplate      车牌号码
    * @param wncar         白名单车辆
    * @return (isFatigueDrive,speed)
    *         isFatigueDrive:是否疲劳驾驶，0代表正常，1代表两点到五点之间上高速，#2代表两点前上高速，五点后下高速
    *         speed：预估速度 (km/h)
    */
  def validateDrive(enTime: String, exTime: String, miles: String, standardSpeed: Double, carplate: String, wncar: Set[String]): (String, String) = {
    val inHour = enTime.substring(11, 13).toInt
    val outHour = exTime.substring(11, 13).toInt
    val inday = DateUtil.strToDateYMD(enTime).getTime
    val outday = DateUtil.strToDateYMD(exTime).getTime
    if (!wncar.contains(carplate) && (inHour >= 2 && inHour <= 4) && (outHour >= 2 && outHour <= 4)) {
      val speed = calcSpeed1(miles, getBetweenTimes(enTime, exTime))
      (String.valueOf(1), speed)
    } else if (!wncar.contains(carplate) && inHour >= 2 && inHour <= 4) {
      val speed = calcSpeed1(miles, getBetweenTimes(enTime, exTime))
      (String.valueOf(1), speed)
    } else if (!wncar.contains(carplate) && outHour >= 2 && outHour <= 4) {
      val speed = calcSpeed1(miles, getBetweenTimes(enTime, exTime))
      (String.valueOf(1), speed)
    } else if (!wncar.contains(carplate) && (inHour < 2 && outHour >= 5 || (inHour >= 5 && outHour >= 5 && outday > inday))) {
      val speed = calcSpeed2(miles, getBetweenTimes(enTime, exTime))
      if (speed.toDouble > standardSpeed) {
        (String.valueOf(1), speed)
      } else {
        (String.valueOf(0), speed)
      }
    } else {
      val speed = calcSpeed1(miles, getBetweenTimes(enTime, exTime))
      (String.valueOf(0), speed)
    }
  }

  /**
    * 替换无意义字符
    *
    * @param str
    * @return
    */
  def repaces(str: String): String = {
    val reStr = if (StringUtils.isNotBlank(str)) {
      str.replaceAll("\\s*", "").replaceAll("\"", "").trim
    } else {
      str
    }
    reStr
  }

  /**
    * 计算速度 单位： km/h
    */
  def calcSpeed1(miles: String, seconds: String): String = {
    val miles_ = miles.toDouble //行驶公里数
    val driveTime = seconds.toDouble / 3600
    if (driveTime <= 0) return "0"
    val speed = miles_ / driveTime
    f"$speed%1.2f".toString //保留两位小数
  }

  /**
    * 计算速度 单位： km/h
    */
  def calcSpeed2(miles: String, seconds: String): String = {
    val miles_ = miles.toDouble //行驶公里数
    val driveTime = (seconds.toDouble / 3600) - 3.toDouble //假设中途休息三个小时
    if (driveTime <= 0) return "0"
    val speed = miles_ / driveTime

    f"$speed%1.2f".toString //保留两位小数
  }

  /**
    * 秒转分钟
    *
    * @param seconds
    * @return
    */
  def secondToMin(seconds: String): String = {
    val min = seconds.toDouble / 60.toDouble
    f"$min%1.2f".toString //保留两位小数
  }

  /**
    * 米转千米
    *
    * @param metre
    * @return
    */
  def metreToKm(metre: String): String = {
    val km = metre.toDouble / 1000.toDouble
    f"$km%1.2f".toString //保留两位小数
  }
}





















