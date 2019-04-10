package com.data.split

import java.io.File
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.ArrayList

import com.data.database.HBaseClient
import com.data.storage.GridDataToHBase
import com.spark.utils.FileUtil
import org.apache.hadoop.hbase.util.Bytes

/**
  * @Auther: liyongchang
  * @Date: 2018/12/3 11:32
  * @Description:
  */
object Test {

  def transformTileList(pollutionTileList: ArrayList[ArrayList[(Int, Array[Float], (Double, Double), Double, (Int, Int), (Int, Int), Double)]])={

    val resultList = new ArrayList[(Int, ArrayList[Array[Float]], (Double, Double), Double, (Int, Int), (Int, Int), Double)]

    val tileList = pollutionTileList.get(0)

    for (i <- Range(0, tileList.size())){

      val dataList = new ArrayList[Array[Float]]

      for (j <- Range(0, pollutionTileList.size())){

        dataList.add(pollutionTileList.get(j).get(i)._2)

      }
      resultList.add( (tileList.get(i)._1, dataList, tileList.get(i)._3, tileList.get(i)._4, tileList.get(i)._5, tileList.get(i)._6, tileList.get(i)._7))

    }
    resultList

  }

  def arrayToList(array: Array[String])={

    val list = new ArrayList[String]

    for (str <- array){
      list.add(str)
    }
    list
  }


  /**
    * 返回hashMap，key为省市名称，value为rowKey
    */
  def getProvinceCityInfoFromHBase()={

    val resultMap = new util.HashMap[String, String]()

    val startRowKey = Bytes.toBytes("0000")
    val endRowKey = Bytes.toBytes("9999")

    val fc = Array(("cf1", Array("province", "city", "lnglat")))

    val selectResults = HBaseClient.scanTable("province_city1", startRowKey, endRowKey, fc)

    for (i <- Range(0, selectResults.size())){

      val rowKey = Bytes.toString(selectResults.get(i)._1)

      val hashMap = selectResults.get(i)._2

      val province = Bytes.toString(hashMap.get("cf1:province"))
      val city = Bytes.toString(hashMap.get("cf1:city"))
      val lnglat = Bytes.toString(hashMap.get("cf1:lnglat"))

      //println(rowKey + "  " + province + ":" + city + "  " + lnglat)
      resultMap.put(province + ":" + city, rowKey)

    }
    resultMap
  }


  def insertAirDataByPath(filePath: String, startDate: String, endDate: String)={

    val dir = new File(filePath)

    val files = FileUtil.getFiles1(dir)

    val provinceCityMap = getProvinceCityInfoFromHBase()

    val tileLength =16

    for (file <- files){

      val fileName = file.getName
      val provinceName = fileName.split("%")(0)

      var cityName = "*"
      val cityArray = fileName.split("#")(0).split("%")
      if (cityArray.length > 1){
        cityName = cityArray(1)
      }else{
        cityName = provinceName
      }
      val dateArr = fileName.split("#")(1).split("H")
      val date = dateArr(0) + " " + dateArr(1) + ":00:00"

      //只插入规定时间范围内的数据
      if (date >= startDate && date <= endDate){
        val pollution = fileName.split("#")(2)
        val pollutionList = new Array[String](1)
        pollutionList(0) = pollution.split("\\.")(0)
        val pollutionTileList = new ArrayList[ArrayList[(Int, Array[Float], (Double, Double), Double, (Int, Int), (Int, Int), Double)]]


        val tileSplit = new TileSplit(filePath + "/" + fileName)
        tileSplit.readAscFileAndInitData()
        val tileResult = tileSplit.splitTile(tileLength)
        val hilbertTileList = tileSplit.tileNumToHilbertCode(tileResult)
        pollutionTileList.add(hilbertTileList)


        val tileList = transformTileList(pollutionTileList)

        val gridDataToHBase = new GridDataToHBase
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val d = format.parse(date)
        val timestamp = new Timestamp(d.getTime()).getTime/1000
        var provinceCityNo = ""
        if (provinceCityMap.get(provinceName + ":" + cityName) != null){
          provinceCityNo = provinceCityMap.get(provinceName + ":" + cityName)
        }else{
          provinceCityNo = provinceCityMap.get(provinceName + ":" + cityName + "市")
        }

        val provinceNo = provinceCityNo.substring(0, 2).toLong
        val cityNo = provinceCityNo.substring(2,4).toLong

        val propertiesAsc = tileSplit.getPropertiesFromAsc()
        gridDataToHBase.insertProperties(provinceNo, cityNo, propertiesAsc, tileLength)

        gridDataToHBase.addData(timestamp, provinceNo, cityNo, tileList, arrayToList(pollutionList))
        println("载入数据：" + fileName)
      }


    }
    println("全部完成！")

  }


  def main(args: Array[String]):Unit={

//    val hilbert = new HilbertTransform()
//
//    //插入数据
//    val pollutionList = Array("aqi", "pm25", "pm10", "co", "no2", "so2")
//
//    val time = "2016-11-11 08:00:00"
//
//    val province = "北京"
//
//    val city = "北京"
//
//    val pollutionTileList = new ArrayList[ArrayList[(Int, Array[Float], (Double, Double), Double, (Int, Int), (Int, Int), Double)]]
//
//    for (i <- Range(0, pollutionList.length)){
//      val tileSplit = new TileSplit("data/浙江%杭州#2016-06-01H00#pm25.asc")
//      tileSplit.readAscFileAndInitData()
//      val tileResult = tileSplit.splitTile(16)
//      val hilbertTileList = tileSplit.tileNumToHilbertCode(tileResult)
//      pollutionTileList.add(hilbertTileList)
//    }
//
//    val tileList = transformTileList(pollutionTileList)
//
//    val gridDataToHBase = new GridDataToHBase
//    val date = "2016-06-12 23:00:00"
//    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val d = format.parse(date)
//    val timestamp = new Timestamp(d.getTime()).getTime/1000
//    gridDataToHBase.addData(timestamp, 1, 1, tileList, arrayToList(pollutionList))
//
    //println(getProvinceCityInfoFromHBase())
    //val filePath = args(0)


    val startDate = "2015-01-01"
    val endDate = "2017-12-32"

    //insertAirDataByPath("K:\\毕设\\实验\\数据\\插值\\湖北-武汉-aqi-pm25-pm10", startDate, endDate)
    insertAirDataByPath("K:\\毕设\\实验\\数据\\插值\\湖北-武汉-co-no2-so2", startDate, endDate)
    insertAirDataByPath("K:\\毕设\\实验\\数据\\插值\\湖北-其它-pm25", startDate, endDate)
    insertAirDataByPath("K:\\毕设\\实验\\数据\\插值\\湖北省-pm25", startDate, endDate)

    //insertAirDataByPath("K:\\毕设\\实验\\数据\\插值\\test", startDate, endDate)


  }

}
