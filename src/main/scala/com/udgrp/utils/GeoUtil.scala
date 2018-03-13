
package com.udgrp.utils

/**
  * Created by kejw on 2017/10/12.
  */
object GeoUtil {

  /**
    * 计算两个经纬度之间的距离
    *
    * @param lon1 经度1
    * @param lat1 纬度1
    * @param lon2 经度2
    * @param lat2 纬度2
    * @return 距离（m）
    */
  def distance(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Double = {
    val earth_radius = 6367000
    val hSinY = Math.sin((lat1 - lat2) * Math.PI / 180 * 0.5)
    val hSinX = Math.sin((lon1 - lon2) * Math.PI / 180 * 0.5)
    val s = hSinY * hSinY + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * hSinX * hSinY
    2 * Math.atan2(Math.sqrt(s), Math.sqrt(1 - s)) * earth_radius
  }
}
