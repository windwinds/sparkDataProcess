package com.spark.bootstrap

import com.data.storage.GridDataToHBase
import com.spark.config.SparkConfig
import com.spark.hbase.SparkToHBase._
import com.spark.solap.SpatioTemporalQuery
import org.apache.hadoop.hbase.util.Bytes

/**
  * @Auther: liyongchang
  * @Date: 2019/3/21 17:37
  * @Description:
  */
object StartQuery {

  def main(args: Array[String]):Unit={

//    val startDate = "2015-01-01 00:00:00"
//    val endDate = "2017-12-32 00:00:00"
//    val province = "湖北"
//    val city = "武汉"
    if (args.length != 7){
      println("需要参数：startDate、endDate、province、pollution、timeDimensionHierarchy、tableName")
      println("eg：2015-01-01 00:00:00、2017-12-32 00:00:00、湖北、武汉、pm25、day、gridAirData_ALL")
    }

    val startDate = args(0)
    val endDate = args(1)
    val province = args(2)
    val city = args(3)
    val pollution = args(4)
    val timeDimensionHierarchy = args(5)

    val tableName = args(6)

    val SparkConfig = new SparkConfig()
    val sc = SparkConfig.getSparkContext("stQuery")
    val fc = Array(("aqi", Array("data")), ("pm25", Array("data")),("pm10", Array("data")),("co", Array("data")),("no2", Array("data")),("so2", Array("data")),("properties", Array("lng", "lat", "cellSize", "x", "y", "hight", "width", "noData")))

    val ascInfoBroadCast = broadCastAscInfo(sc)

    val startTimeStamp = dateToTimeStamp(startDate)
    val endTimeStamp = dateToTimeStamp(endDate)
    val gridDataToHBase = new GridDataToHBase()

    val provinceCity = getProvinceAndCityNoByName(province, city)
    val startRowKey = gridDataToHBase.generateRowKeyNoHilbertCode(provinceCity._1, provinceCity._2)
    val stopRowKey = gridDataToHBase.generateRowKeyNoHilbertCode(provinceCity._1, provinceCity._2 + 1)

    val tableRdd = getRddByScanTable(sc, tableName, Bytes.toBytes(startRowKey), Bytes.toBytes(stopRowKey),startTimeStamp,endTimeStamp, fc)
    //println(tableRdd.count())
    //val propertyMap = getPropertiesFromTableRdd(tableRdd)
    //printHashMap(propertyMap)

    //tableRdd.persist()
    //tableRdd.persist()
    tableRdd.count()
//    val tileDataRdd = getTileDataWithPropertyRddFromTableRdd(tableRdd, pollution)
//    tileDataRdd.persist()
//    tileDataRdd.count()
//
//    val spatioTemporalQuery = new SpatioTemporalQuery()


    //时间查询
//    val resultRdd = spatioTemporalQuery.regularPatternInDay(tileDataRdd, timeDimensionHierarchy)
//    resultRdd.count()
//    resultRdd.coalesce(1).saveAsTextFile("file:///home/lyc/samba/query_result")


    //空间查询
//    val tileMapRdd = spatioTemporalQuery.spatialQuery(tileDataRdd, "")
//    tileMapRdd.count()
//    val ascRdd = spatioTemporalQuery.tileToAscRaster(tileMapRdd, ascInfoBroadCast.value)
//    ascRdd.coalesce(1).map(kv=>kv._2).saveAsTextFile("file:///home/lyc/samba/lyc/asc_result")

    //resultRdd.saveAsTextFile("file:///home/lyc/samba/output.txt")

//    val pollutions = Array("aqi", "pm25", "pm10", "co", "no2", "so2")
//    for (pollution <- pollutions){
//      val tileDataRdd = getTileDataRddFromTableRdd(tableRdd, pollution)
//      val spatioTemporalQuery = new SpatioTemporalQuery()
//      val resultRdd = spatioTemporalQuery.regularPatternInDay(tileDataRdd, "year")
//      println(pollution + "---------------------------------------------------------------------------------")
//      resultRdd.foreach(result => {
//          //println("(" + result._1 + ", " + result._2 + ")")
//          println(result._2.formatted("%.1f"))
//
//      })
//    }

  }

}
