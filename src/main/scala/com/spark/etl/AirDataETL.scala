package com.spark.etl

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.io.Source

class AirDataETL extends Serializable {

  val columns = Array("time", "city", "site", "aqi", "pm2.5", "pm2.5_24h", "pm10", "pm10_24h",
    "so2", "so2_24h", "no2", "no2_24h", "o3", "o3_24h", "o3_8h", "o3_8h_24h", "co",
    "co_24h")
  val cityFile = "data/province_city.txt"
  val locationFile = "data/location_wgs84.txt"

  /**
    * 判断数组中包含空值的数据
    * @param array
    * @return
    */
  def isContainNull(array: Array[String]):Boolean={

    //2016.6月之后的都不是满字段
//    if (array.length < columns.length)
//      return false
    if (array(0).equals("time"))
      return false
//    for (value <- array){
//      if (value == null || value == "" || value.trim().isEmpty)
//        return false
//    }
    return true
  }


  /**
    * 去除rdd中不全的数据
    * @param inputRDD
    * @return
    */
  def cleanWrongData(inputRDD: RDD[String]):RDD[Array[String]]={

    inputRDD.map(x => x.split(",")).filter(isContainNull)

  }

  def arrayToString(array: Array[String]):String={

    var result:String = ""
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

    val columnIdArray = new Array[Int](pollutions.length)
    var index = 0
    for (pollution <- pollutions){
      var flag = true
      for (i <- Range(0, columns.length) if flag){
        if (columns(i).equals(pollution)){
          columnIdArray(index) = i
          index += 1
          flag = false
        }
      }
    }
    val resultRdd = inputRdd.map(array => selectArrayByName(array, columnIdArray))
    return resultRdd
  }

  def selectArrayByName(array: Array[String], idArray: Array[Int]): Array[String] ={
    val len = idArray.length
    val result = new Array[String](len)
    for (i <- Range(0, len)){
      result(i) = array(idArray(i))
    }
    return result
  }

  /**
    * 将RDD作为字符串输出
    * @param inputRdd
    * @param num: 输出条数
    */
  def printRdd(inputRdd: RDD[Array[String]], num: Int):Unit={

    inputRdd.map(arrayToString).top(num).foreach(println)

  }

  /**
    * 读取省市文件，得到市为key，省为value的Map
    * @return
    */
  def getCityToProvinceMap():util.HashMap[String, String]={
    val hashMap = new util.HashMap[String, String]()
    val file = Source.fromFile(cityFile)
    for (line <- file.getLines()){
      val str = line.split(" ")
      hashMap.put(str(1), str(0))
    }
    hashMap
  }

  /**
    * 在输入的rdd中加上城市对应省的信息
    * @param inputRdd
    * @param cityMap
    * @return
    */
  def addProvinceForRdd(inputRdd: RDD[Array[String]],
                        cityMap: util.HashMap[String, String]):RDD[Array[String]]={
    inputRdd.filter(s => s.length > 1).map(stringArray => {
      val cityName = stringArray(1)
      val provinceName = cityMap.get(cityName)
      val resultArray = new Array[String](stringArray.length+1)
      resultArray(0) = stringArray(0)
      resultArray(1) = provinceName
      for (i <- Range(1, stringArray.length)){
        resultArray(i+1) = stringArray(i)
      }
      resultArray
    })
  }

  /**
    * 得到rdd中所有地址，不能有重复
    * @param inputRdd
    * @return
    */
  def getAddressArrayFromRdd(inputRdd: RDD[Array[String]]):Array[String]={
    val addressRdd = inputRdd.map(stringArray => stringArray(1) + stringArray(2) + stringArray(3)).distinct()
    val resultArray = addressRdd.collect()
    resultArray
  }


  /**
    * 读取位置文件，得到地址为key，经纬度为value的Map
    * @return
    */
  def getAddressToCoordinateMap():util.HashMap[String, Array[String]]={
    val hashMap = new util.HashMap[String, Array[String]]()
    val file = Source.fromFile(locationFile)
    for (line <- file.getLines()){
      val str = line.split(" ")
      hashMap.put(str(0), Array(str(1), str(2)) )
    }
    hashMap
  }

  /**
    * 在添加省信息后的rdd基础上，加上经纬度坐标信息
    * @param inputRdd
    * @param addressMap
    * @return
    */
  def addLngAndLatForRdd(inputRdd: RDD[Array[String]],
                         addressMap: util.HashMap[String, Array[String]]):RDD[Array[String]]={
    inputRdd.filter(s => s.length > 5).map(stringArray => {
      val address = stringArray(1) + stringArray(2) + stringArray(3)
      val lngLatArray = addressMap.get(address)
      val resultArray = new Array[String](stringArray.length+2)
      for (i <- Range(0, 4)){
        resultArray(i) = stringArray(i)
      }
      if (lngLatArray == null){
        resultArray(4) = ""
        resultArray(5) = ""
      }else{
        resultArray(4) = lngLatArray(0)
        resultArray(5) = lngLatArray(1)
      }
      for (i <- Range(4, stringArray.length)){
        resultArray(i+2) = stringArray(i)
      }
      resultArray
    })
  }

  def saveRddInFile(inputRdd: RDD[Array[String]], path: String):Unit={
    inputRdd.map(arrayToString).saveAsTextFile(path)
  }


  def filterRdd(strArray: Array[String]):Boolean={
    for (i <- Range(0, strArray.length)){
      if (strArray(i)==null || strArray(i).trim.isEmpty || strArray(i).equals("_"))
        return false
    }
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val compareTime = dateFormat.parse("2016-07-01 00:00:00")
    val time = dateFormat.parse(strArray(0))
    if (compareTime.compareTo(time) > 0){
      if (strArray.length < 20){
        return false
      }
    }else{
      if (strArray.length < 16){
        return false
      }
    }
    val pattern = "^([1-9]+(\\.\\d+)?|0\\.\\d+)$"
    if (strArray(6).matches(pattern) == false){
      return false
    }
    for (i <- Range(9, strArray.length)){
      if (strArray(i).matches(pattern) == false){
        return false
      }
    }
    return true
  }


  /**
    * 将输入rdd转换为统一形式AirData，2016-07及其之后的数据和之前的数据格式不一样,
    * 需要转换为统一格式
    * @param inputRdd
    * @return
    */
  def transformRdd(inputRdd: RDD[Array[String]]):RDD[AirData]={

    inputRdd.filter(filterRdd).map(stringArray => {

      val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val compareTime = dateFormat.parse("2016-07-01 00:00:00")
      val time = dateFormat.parse(stringArray(0))
      if (compareTime.compareTo(time) > 0){  //时间在2016-07-01之前
//        if (stringArray.length < 20){
//          new AirData("", "", "", "", 0.0, 0.0, 0.0, 0.0,
//            0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
//        }
        new AirData(stringArray(0), stringArray(1), stringArray(2), stringArray(3),
          stringArray(4).toDouble, stringArray(5).toDouble,
          stringArray(6).toDouble, stringArray(7).toDouble, stringArray(9).toDouble,
          stringArray(19).toDouble, stringArray(13).toDouble,
          stringArray(15).toDouble,stringArray(17).toDouble,stringArray(11).toDouble)
      }else{
//        if (stringArray.length < 16){
//          new AirData("", "", "", "", 0.0, 0.0, 0.0, 0.0,
//            0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
//        }
        new AirData(stringArray(0), stringArray(1), stringArray(2), stringArray(3),
          stringArray(4).toDouble, stringArray(5).toDouble,
          stringArray(6).toDouble, stringArray(9).toDouble, stringArray(10).toDouble,
          stringArray(11).toDouble, stringArray(12).toDouble,
          stringArray(13).toDouble,stringArray(14).toDouble,stringArray(15).toDouble)
      }
    })
  }



//  def rddToDataFrame(inputRdd: RDD[AirData]):DataFrame={
//
//    inputRdd.toDF()
//
//  }


}

//DataFrame 表结构
case class AirData(time: String, province: String, city: String, site: String,
                   lng: Double, lat: Double,
                   aqi: Double, pm25: Double, pm10: Double, co: Double, no2: Double,
                   ozone1hour: Double, ozone8hour: Double, so2: Double)
