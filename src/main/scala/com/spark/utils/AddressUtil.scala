package com.spark.utils


import java.io.{File, PrintWriter}

import com.alibaba.fastjson.JSON

import scala.io.Source


/**
  * @Auther: liyongchang
  * @Date: 2018/11/5 15:07
  * @Description:
  */
object AddressUtil {

  val key = "QUk7wy07EobDWQ7skwof0YAFOSQRjFPC"
  val locationFile = "data/location_wgs84.txt"

  //坐标转换用到的常量
  val x_PI = 3.14159265358979324 * 3000.0 / 180.0
  val PI = 3.1415926535897932384626
  val a = 6378245.0
  val ee = 0.00669342162296594323

  /**
    * 通过地址查询经纬度，结果写入文件
    * @param addressArray
    */
  def getLocationByAddress(addressArray: Array[String]):Unit={

    val file = new File(locationFile)
    val writer = new PrintWriter(file)
    writer.println("address " + "lng " + "lat")
    var i = 1
    for (address <- addressArray){
      val url = "http://api.map.baidu.com/geocoder/v2/?address="+address+"&output=json&ret_coordtype=gcj02ll&ak="+key
      val httpResponse = Source.fromURL(url).mkString
      val json = JSON.parseObject(httpResponse)
      var lng = 0.0
      var lat = 0.0
      if (json.containsKey("result")){
        val lngG = json.getJSONObject("result").getJSONObject("location").get("lng").toString
        val latG = json.getJSONObject("result").getJSONObject("location").get("lat").toString
        lng = gcj02towgs84(lngG.toDouble, latG.toDouble)._1
        lat = gcj02towgs84(lngG.toDouble, latG.toDouble)._2
        println(i + " http request success")
      }else{
        println(i + " http request failed")
      }
      writer.println(address + " " + lng + " " + lat)
      i+=1
    }
    writer.close()
  }

  def transformlat(lng: Double, lat: Double):Double={
    var ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + 0.1 * lng * lat + 0.2 * Math.sqrt(Math.abs(lng))
    ret += (20.0 * Math.sin(6.0 * lng * PI) + 20.0 * Math.sin(2.0 * lng * PI)) * 2.0 / 3.0
    ret += (20.0 * Math.sin(lat * PI) + 40.0 * Math.sin(lat / 3.0 * PI)) * 2.0 / 3.0;
    ret += (160.0 * Math.sin(lat / 12.0 * PI) + 320 * Math.sin(lat * PI / 30.0)) * 2.0 / 3.0
    ret
  }

  def transformlng(lng: Double, lat: Double):Double={
    var ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + 0.1 * lng * lat + 0.1 * Math.sqrt(Math.abs(lng))
    ret += (20.0 * Math.sin(6.0 * lng * PI) + 20.0 * Math.sin(2.0 * lng * PI)) * 2.0 / 3.0
    ret += (20.0 * Math.sin(lng * PI) + 40.0 * Math.sin(lng / 3.0 * PI)) * 2.0 / 3.0
    ret += (150.0 * Math.sin(lng / 12.0 * PI) + 300.0 * Math.sin(lng / 30.0 * PI)) * 2.0 / 3.0
    ret
  }

  def gcj02towgs84(lng: Double, lat: Double)={
    var dlat = transformlat(lng - 105.0, lat - 35.0)
    var dlng = transformlng(lng - 105.0, lat - 35.0)
    var radlat = lat / 180.0 * PI
    var magic = Math.sin(radlat)
    magic = 1 - ee * magic * magic
    var sqrtmagic = Math.sqrt(magic)
    dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * PI)
    dlng = (dlng * 180.0) / (a / sqrtmagic * Math.cos(radlat) * PI)
    var mglat = lat + dlat
    var mglng = lng + dlng
    (lng * 2 - mglng, lat * 2 - mglat)
  }

  def main(args: Array[String]):Unit={

    val address = "新疆昌吉州天山天池"
    var url = "http://api.map.baidu.com/geocoder/v2/?address="+address+"&output=json&ak="+key
    val result = Source.fromURL(url).mkString
    val json = JSON.parseObject(result)
    val lng = json.getJSONObject("result").getJSONObject("location").get("lng")
    val lat = json.getJSONObject("result").getJSONObject("location").get("lat")
    println(result)
    println(lng)
    println(lat)

  }

}
