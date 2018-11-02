package com.spark.etl

import org.apache.spark.rdd.RDD

class AirDataETL extends Serializable {

  val columns = Array("time", "city", "site", "aqi", "pm2.5", "pm2.5_24h", "pm10", "pm10_24h",
    "so2", "so2_24h", "no2", "no2_24h", "o3", "o3_24h", "o3_8h", "o3_8h_24h", "co",
    "co_24h")

  /**
    * 判断数组中包含空值的数据
    * @param array
    * @return
    */
  def isContainNull(array: Array[String]):Boolean={

    if (array.length < columns.length)
      return false

    for (value <- array){
      if (value == null || value == "")
        return false
    }
    return true
  }


  //删除RDD中不全的数据
  def cleanWrongData(inputRDD: RDD[String]):RDD[Array[String]]={

    inputRDD.map(x => x.split(",")).filter(isContainNull)

  }

  def arrayToString(array: Array[String]):String={

    var result = ""
    for (i <- Range(0, array.length)){
      if (i < array.length-1){
        result += array(i) + ","
      }else{
        result += array(i)
      }
    }
    return result
  }

  /**
    * 得到指定污染物的RDD（"time", "city", "site"一直包含）
    * @param inputRdd
    * @param pollutions
    * @return
    */
  def getRddByPollutionName(inputRdd: RDD[Array[String]], pollutions: Array[String]): RDD[Array[String]]={

//    inputRdd.map()
    return inputRdd
  }

  def selectArrayByName(array: Array[String], idArray: Array[Int]): Array[String] ={
    val len = idArray.length
    val result = new Array[String](len)
    for (i <- Range(0, len)){
      result(i) = array(idArray(i))
    }
    return result
  }


}
