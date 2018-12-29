package com.data.split

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.ArrayList

import com.data.storage.GridDataToHBase

/**
  * @Auther: liyongchang
  * @Date: 2018/12/3 11:32
  * @Description:
  */
object Test {

  def transformTileList(pollutionTileList: ArrayList[ArrayList[(Int, Array[Float], (Double, Double), Double, (Int, Int), (Int, Int))]])={

    val resultList = new ArrayList[(Int, ArrayList[Array[Float]], (Double, Double), Double, (Int, Int), (Int, Int))]

    val tileList = pollutionTileList.get(0)

    for (i <- Range(0, tileList.size())){

      val dataList = new ArrayList[Array[Float]]

      for (j <- Range(0, pollutionTileList.size())){

        dataList.add(pollutionTileList.get(j).get(i)._2)

      }
      resultList.add( (tileList.get(i)._1, dataList, tileList.get(i)._3, tileList.get(i)._4, tileList.get(i)._5, tileList.get(i)._6))

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

  def main(args: Array[String]):Unit={

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

    val pollutionTileList = new ArrayList[ArrayList[(Int, Array[Float], (Double, Double), Double, (Int, Int), (Int, Int))]]

    for (i <- Range(0, pollutionList.length)){
      val tileSplit = new TileSplit("data/testTile.asc")
      tileSplit.readAscFileAndInitData()
      val tileResult = tileSplit.splitTile(2)
      val hilbertTileList = tileSplit.tileNumToHilbertCode(tileResult)
      pollutionTileList.add(hilbertTileList)
    }

    val tileList = transformTileList(pollutionTileList)

    val gridDataToHBase = new GridDataToHBase
    val date = "2016-06-12 08:00:00"
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d = format.parse(date)
    val timestamp = new Timestamp(d.getTime()).getTime/1000
    gridDataToHBase.addData(timestamp, 1, 1, tileList, arrayToList(pollutionList))



  }

}
