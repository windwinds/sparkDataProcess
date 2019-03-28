package com.data.split

import java.io.{File, PrintWriter, RandomAccessFile}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.ArrayList
import java.util.regex.Pattern

import breeze.linalg.DenseMatrix
import com.data.database.HBaseClient
import com.data.storage.GridDataToHBase
import com.spark.utils.DataTransformUtil
import org.apache.hadoop.hbase.util.Bytes
import src.main.scala.com.data.storage.{TileRange, TilesRangeHbase}
import src.main.scala.com.data.storage.TileRange.getLnglatofEachTile

/**
  * @Auther: liyongchang
  * @Date: 2018/12/3 11:32
  * @Description:
  */
object Test {

  def transformTileList(pollutionTileList: ArrayList[ArrayList[(Int, Array[Float], (Double, Double), Double, (Int, Int), (Int, Int))]]) = {

    val resultList = new ArrayList[(Int, ArrayList[Array[Float]], (Double, Double), Double, (Int, Int), (Int, Int))]

    val tileList = pollutionTileList.get(0)

    for (i <- Range(0, tileList.size())) {

      val dataList = new ArrayList[Array[Float]]

      for (j <- Range(0, pollutionTileList.size())) {

        dataList.add(pollutionTileList.get(j).get(i)._2)

      }
      resultList.add((tileList.get(i)._1, dataList, tileList.get(i)._3, tileList.get(i)._4, tileList.get(i)._5, tileList.get(i)._6))

    }
    resultList

  }

  def arrayToList(array: Array[String]) = {

    val list = new ArrayList[String]

    for (str <- array) {
      list.add(str)
    }
    list
  }

  /**
    * 返回hashMap，key为省市名称，value为（rowKey, lng, lat）
    */
  def getProvinceCityInfoFromHBase(): Unit = {

    val startRowKey = Bytes.toBytes("0000")
    val endRowKey = Bytes.toBytes("9999")

    val fc = Array(("cf1", Array("province", "city", "lnglat")))

    val selectResults = HBaseClient.scanTable("province_city1", startRowKey, endRowKey, fc)

    for (i <- Range(0, selectResults.size())) {

      val rowKey = Bytes.toString(selectResults.get(i)._1)

      val hashMap = selectResults.get(i)._2

      val province = Bytes.toString(hashMap.get("cf1:province"))
      val city = Bytes.toString(hashMap.get("cf1:city"))
      val lnglat = Bytes.toString(hashMap.get("cf1:lnglat"))

      println(rowKey + "  " + province + ":" + city + "  " + lnglat)


    }

  }
  def transDate(date:String) ={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d = format.parse(date)
    //timestamp为10位的时间戳
    val timestamp = new Timestamp(d.getTime()).getTime / 1000
    timestamp
  }

  def main(args: Array[String]): Unit = {
    val hilbert = new HilbertTransform()
    //    val n = 8
    //    for (i <- Range(0, n)){
    //      for (j <- Range(0, n)){
    //        val d = hilbert.xyToHilbertCode(n, j, n-i-1)
    //        print(d + " ")
    //      }
    //      println()
    //    }
    //    for (n <- Range(0, 100)){
    //      val d = hilbert.xyToHilbertCode(n, 0, 0)
    //      print(d + " ")
    //    }

    //    for (i <- Range(0, 64)){
    //      val xy = hilbert.hilbertCodeToXY(n, i)
    //      println(xy._1, xy._2)
    //    }

    //插入数据
    val pollutionList = Array("aqi", "pm25", "pm10", "co", "no2", "ozone1hour", "ozone8hour", "so2")

    val time = "2016-11-11 08:00:00"

    val province = "北京"

    val city = "北京"
    var NODATA_value = 0.toDouble
    val pollutionTileList = new ArrayList[ArrayList[(Int, Array[Float], (Double, Double), Double, (Int, Int), (Int, Int))]]

    for (i <- Range(0, pollutionList.length)) {
      val tileSplit = new TileSplit("data/hubei-20")
      tileSplit.readAscFileAndInitData()
      NODATA_value = tileSplit.NODATA_value

      //瓦片划分结果tileResult为((0,0),[F@7a9273a8,(115.41381,39.441978),0.01,(16,16),(11,14))元组列表
      val tileResult = tileSplit.splitTile(16)

      //转希尔后的
      val hilbertTileList = tileSplit.tileNumToHilbertCode(tileResult)
        pollutionTileList.add(hilbertTileList)
      }

     //转为一维后的
      val tileList = transformTileList(pollutionTileList)
      val gridDataToHBase = new GridDataToHBase
      val date = "2016-06-12 08:00:00"
    //timestamp为10位的时间戳
      val timestamp = transDate(date)
      gridDataToHBase.addData(timestamp, 1, 1, tileList, arrayToList(pollutionList))


    //方法1：从gridDataToHBase中找到所有瓦片的properties读出来
    val maxHilbert = gridDataToHBase.getMaxHilbertCode(timestamp, 1, 1)
    val allList = gridDataToHBase.getAllTileList(timestamp, 1, 1,maxHilbert.toLong)
    val all_Map = gridDataToHBase.tileToMap(allList)
//    //查找某一点的瓦片数据
//    val number = TileRange.getNumber("故宫博物院",allList) //查找该位置对应的hilbert值
//    val data = gridDataToHBase.searchData(timestamp, 1, 1,number,arrayToList(pollutionList),Bytes.toBytes("data"))//从gridAirData表中查找该瓦片
//    //查找某一个范围的瓦片数据
//    val all_Number = TileRange.getAllNumber(("115.41381","39.441978"),("116.41381","40.541978"),allList)
    //瓦片拼接
    val tileSplicing = new TileSplicing()
    val all_Number = new util.ArrayList[Int]
    for (i <- Range(0, maxHilbert+1)){
      if(all_Map.contains(i)){
        all_Number.add(i)
      }
    }
    val startTime = System.currentTimeMillis()
    tileSplicing.tileSplicing(all_Number,timestamp, 1, 1,"aqi",all_Map,NODATA_value)
   println(System.currentTimeMillis()-startTime)





    //val allldata = new ArrayList[(Int,ArrayList[(String,Array[Float])])]


//    for( i <- Range(0,all_Number.size())){//遍历all_Number，查找瓦片数据并拼接
//      val t = new TilesRangeHbase
//      val hilbertN = t.getHilbertN(timestamp, 1, 1)
//      val site = hilbert.hilbertCodeToXY(hilbertN,all_Number.get(i))//转为坐标
//      //从gridAirData表中查找该瓦片
//      val data = gridDataToHBase.searchData(timestamp, 1, 1,all_Number.get(i),arrayToList(pollutionList),Bytes.toBytes("data"))
//      allldata.add((all_Number.get(i),data))
//      println(all_Number.get(i),data)
//     }



//    //方法2：构建Hbase表
//    val tHbase = new TilesRangeHbase    //构建Hbase表，并插入数据
//    HBaseClient.create_table("TileRange1",Array("cf1") )
//    val tileResult_ChangeLnglat= TileRange.getLnglatofEachTile(tHbase.delect_data(tileList))
//    tHbase.tileRangeHbase(TileRange.transformDate(date),"01","01",tileResult_ChangeLnglat)
//    //查找某一点的瓦片数据
//    val r = TileRange.getLatAndLngByAddress("故宫博物院")
//    val geohashcode = new GeoHashTransform().encode(r.get("lat").doubleValue(),r.get("lng").doubleValue())
//    val list = tHbase.similarityMatch(geohashcode)       //前缀匹配
//    val value =tHbase.searchHbase(tHbase.findData(list))    //得到瓦片的hilbert值
//    println(Bytes.toInt(value))
//    //查找某一个范围的瓦片数据
//    val numberList = tHbase.getAllNumber(("115.41381","39.441978"),("116.41381","40.541978"),timestamp, 1, 1) //得到该范围所有的瓦片的hilbert值


//    for(j <- Range(0,6 + 1)){
//      for(i <- Range(4,10 + 1)){
//        //println(i,j)
//        println(hilbert.xyToHilbertCode(16,i,j))
//      }
//    }

    }

}
