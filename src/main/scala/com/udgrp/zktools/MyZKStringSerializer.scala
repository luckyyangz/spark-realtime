
package com.udgrp.zktools

import org.I0Itec.zkclient.exception.ZkMarshallingError
import org.I0Itec.zkclient.serialize.ZkSerializer

/**
  * Created by kejw on 2017/10/12.
  */
class MyZKStringSerializer extends ZkSerializer {

  @throws(classOf[ZkMarshallingError])
  def serialize(data: Object): Array[Byte] = data.asInstanceOf[String].getBytes("UTF-8")

  @throws(classOf[ZkMarshallingError])
  def deserialize(bytes: Array[Byte]): Object = {
    if (bytes == null)
      null
    else
      new String(bytes, "UTF-8")
  }
}