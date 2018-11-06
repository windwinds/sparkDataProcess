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
  val locationFile = "data/location.txt"

  /**
    * 通过地址查询经纬度，结果写入文件
    * @param addressArray
    */
  def getLocationByAddress(addressArray: Array[String]):Unit={

    val file = new File(locationFile)
    val writer = new PrintWriter(file)
    writer.println("address " + "lng " + "lat")
    var i = 1;
    for (address <- addressArray){
      val url = "http://api.map.baidu.com/geocoder/v2/?address="+address+"&output=json&ak="+key
      val httpResponse = Source.fromURL(url).mkString
      val json = JSON.parseObject(httpResponse)
      var lng = ""
      var lat = ""
      if (json.containsKey("result")){
        lng = json.getJSONObject("result").getJSONObject("location").get("lng").toString
        lat = json.getJSONObject("result").getJSONObject("location").get("lat").toString
        println(i + " http request success")
      }else{
        println(i + " http request failed")
      }
      writer.println(address + " " + lng + " " + lat)
      i+=1
    }
    writer.close()
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
