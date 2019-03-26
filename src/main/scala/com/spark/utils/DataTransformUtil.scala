package com.spark.utils

import java.text.SimpleDateFormat
import java.util.Date

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

  /**
    * 时间戳转日期格式
    * detail:表示精确到哪有位：hours, day, month, 季节, 年
    */
  def timestampToDate(timestamp: Long, detail: String)={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val date = fm.format(new Date(timestamp*1000))
    var result = date
    if (detail.equals("hours")){
      result = date.split(":")(0)
    }

    if (detail.equals("day")){
      result = date.split(" ")(0)
    }

    if (detail.equals("month")){
      val arr = date.split("-")
      result = arr(0) + "-" + arr(1)
    }

    if (detail.equals("season")){
      val arr = date.split("-")
      val month = arr(1)
      var season = ""
      if (month >= "01" && month <= "03"){
        season = "*1"
      }
      if (month >= "04" && month <= "06"){
        season = "*2"
      }
      if (month >= "07" && month <= "09"){
        season = "*3"
      }
      if (month >= "10" && month <= "12"){
        season = "*4"
      }
      result = arr(0) + season
    }

    result
  }





}
