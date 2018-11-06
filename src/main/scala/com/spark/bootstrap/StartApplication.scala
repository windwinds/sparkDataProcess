package com.spark.bootstrap

import com.spark.config.SparkConfig
import com.spark.etl.AirDataETL
import com.spark.utils.{AddressUtil, FileUtil}

object StartApplication {

  def main(args: Array[String]):Unit={

    val SparkConfig = new SparkConfig()
    val sc = SparkConfig.getSparkContext("local")
    val inputPath = "data/2015-01.csv"
    val outputPath = "resultData"
    val input = sc.textFile(inputPath)

    val airDataETL = new AirDataETL()
    val cleanData = airDataETL.cleanWrongData(input)

    cleanData.persist()

    //println(cleanData.top(10))

    //得到只包含aqi的rdd
//    val aqiRdd = airDataETL.getRddByPollutionName(cleanData, Array("time", "city", "site","aqi"))
    //测试输出方法
//    airDataETL.printRdd(aqiRdd, 100)
    //FileUtil.deleteLocalFile(outputPath)

    //广播:市-省的map
    val cityToProvince = sc.broadcast(airDataETL.getCityToProvinceMap())
    val addProvinceRdd = airDataETL.addProvinceForRdd(cleanData, cityToProvince.value)
    addProvinceRdd.persist()
    airDataETL.printRdd(addProvinceRdd, 100)
    cleanData.unpersist()

    //得到地点对应的经纬度文件
//    val addressList = airDataETL.getAddressArrayFromRdd(addProvinceRdd)
//    addressList.foreach(println)
//    AddressUtil.getLocationByAddress(addressList)

    //广播:address-location的map
    val addressToLocation = sc.broadcast(airDataETL.getAddressToCoordinateMap())
    val addLocationRdd = airDataETL.addLngAndLatForRdd(addProvinceRdd, addressToLocation.value)
    addLocationRdd.persist()
    airDataETL.printRdd(addLocationRdd, 100)
    addProvinceRdd.unpersist()


  }

}
