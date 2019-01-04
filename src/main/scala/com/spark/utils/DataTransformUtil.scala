package com.spark.utils

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


}
