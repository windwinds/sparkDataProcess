package com.spark.bootstrap

import com.spark.config.SparkConfig
import com.spark.etl.AirDataETL
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * @Auther: liyongchang
  * @Date: 2018/11/9 11:19
  * @Description:
  */
object LocalStart {

  def main(args: Array[String]):Unit={

    val SparkConfig = new SparkConfig()
    val sc = SparkConfig.getSparkContext("local")
    val inputPath = "data/2016-07.csv"
    val input = sc.textFile(inputPath)

    val airDataETL = new AirDataETL()
    //val cleanData = airDataETL.cleanWrongData(input)
    val cleanData = airDataETL.transformEmptyData(input)

    val cityToProvince = sc.broadcast(airDataETL.getCityToProvinceMap())
    val addProvinceRdd = airDataETL.addProvinceForRdd(cleanData, cityToProvince.value)

    val addressToLocation = sc.broadcast(airDataETL.getAddressToCoordinateMap())
    val addLocationRdd = airDataETL.addLngAndLatForRdd(addProvinceRdd, addressToLocation.value)

    val sqlContext = new SQLContext(sc)
    val airDataFrame = sqlContext.createDataFrame(airDataETL.transformRdd(addLocationRdd))
    airDataFrame.registerTempTable("airData")
    val select = sqlContext.sql("select * from airData")
    select.show(100)


  }

}
