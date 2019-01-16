package com.spark.utils

import org.apache.hadoop.hbase.util.Bytes


/**
  * @Auther: liyongchang
  * @Date: 2018/12/14 10:42
  * @Description:
  */
object DataTransformUtil {

  /**
    * 浮点数组，转字节数组
    * @param floatArray
    * @return
    */
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

  /**
    * 字节数组，转浮点数组
    * @param bytesArray
    */
  def bytesArrayToFloatArray(bytesArray: Array[Byte])={

    val len = bytesArray.length
    val result = new Array[Float](len/4)
    var resultIndex = 0
    var i = 0
    while (i < len){

      val floatBytesArray = new Array[Byte](4)
      for (j <- Range(0, 4)){
        floatBytesArray(j) = bytesArray(i)
        i = i + 1
      }
      result(resultIndex) = Bytes.toFloat(floatBytesArray)
      resultIndex+=1
    }
    result
  }



}
