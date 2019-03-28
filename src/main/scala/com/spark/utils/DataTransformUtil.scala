package com.spark.utils

import java.io.{ByteArrayInputStream, DataInputStream, EOFException, IOException}
import java.nio.ByteBuffer

import org.apache.hadoop.hbase.util.Bytes


/**
  * @Auther: liyongchang
  * @Date: 2018/12/14 10:42
  * @Description:
  */
object DataTransformUtil {

  def floatArrayToBytesArray(floatArray: Array[Float])={
    val length = floatArray.length
    val byteArray = new Array[Byte](length*4)
    var index = 0
    for (floatValue <- floatArray){
      val floatBytes = Bytes.toBytes(floatValue)
      for (i <- Range(0, floatBytes.length)){
        byteArray(index) = floatBytes(i)
        index+=1
      }
    }
    byteArray
  }
  def bytesArrayToFloatArray(bytesArray: Array[Byte]) ={
    val floatArray = new Array[Float](bytesArray.length/4)
    var float = new Array[Byte](4)
    var index = 0
    for(i <- Range(0,bytesArray.length,4)) {
      float(0) = bytesArray(i)
      float(1) = bytesArray(i + 1)
      float(2) = bytesArray(i + 2)
      float(3) = bytesArray(i + 3)
      floatArray(index) = ByteBuffer.wrap(float).getFloat()
      index+=1
    }
    floatArray
  }


}
