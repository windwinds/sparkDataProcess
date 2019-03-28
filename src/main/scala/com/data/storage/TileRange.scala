package src.main.scala.com.data.storage

import java.io._
import java.math.BigDecimal
import java.net.{HttpURLConnection, URL}
import java.util
import java.util.{ArrayList}

import com.data.split.HilbertTransform
/*
*@Description:
*@Return:${returns}
*@Author:LiumingYan
*@Date 11:592019/2/27
*/
object TileRange {
  //输入位置名称，返回该瓦片的hilbert编码
  def getNumber(site_name:String ,tileList:ArrayList[(Int, (Double, Double), Double, (Int, Int), (Int, Int))]) ={
    //获取各瓦片的经纬度范围
    val tileResult_ChangeLnglat= getLnglatofEachTile(tileList)
    val tileRange = tileFindRange(tileResult_ChangeLnglat)
    //获取中心经纬度
    val r = getLatAndLngByAddress(site_name)
    val lat = r.get("lat").toString
    val lng = r.get("lng").toString
    println(lat)
    println(lng)
    //确定查找的位置在哪个瓦片上
    val number = search(tileRange,lng,lat)
    number
  }

  //计算每个瓦片的左下的经纬度
  def getLnglatofEachTile(tileList: ArrayList[(Int, (Double, Double), Double, (Int, Int), (Int, Int))]) = {
    if (tileList == null || tileList.size() == 0) {
      println("传入的瓦片数据为空！")
      sys.exit()
    }
    //修改lnglat后的
    val aList = new ArrayList[(Int, (Double, Double), Double, (Int, Int), (Int, Int))]()
    val lnglat = tileList.get(0)._2
    val cellsize = tileList.get(0)._3
    val H = new HilbertTransform()
    for (i <- Range(0, tileList.size())) {
      //hilbert编码转为坐标x,y
      val hilbertCode = tileList.get(i)._1
      val rowColNum = tileList.get(i)._5
      val hilbertN = H.rowNumAndColNumGetN(rowColNum._1, rowColNum._2)
      val site = H.hilbertCodeToXY(hilbertN,hilbertCode)
      val x = site._1
      val y = site._2
      //依据坐标计算每个瓦片左下的经纬度
      val Height = tileList.get(i)._4._1
      val Width = tileList.get(i)._4._2
      val lat: Double = lnglat._2 + (rowColNum._1 - y - 1) * Height * cellsize
      val lng: Double = lnglat._1 + x * Width * cellsize
      aList.add(tileList.get(i)._1, (lng, lat), cellsize, (Height, Width), (rowColNum._1, rowColNum._2))
    }
    aList
  }
  //构建经纬度范围与瓦片编号对应list
  def tileFindRange(tileResult: ArrayList[(Int, (Double, Double), Double, (Int, Int), (Int, Int))]) = {
    val tileRange =  new ArrayList[((BigDecimal, BigDecimal),(BigDecimal, BigDecimal),(Int,Int ),Int)]
    for (i <- Range(0, tileResult.size)){
      //计算每个瓦片的经纬度范围
      val lng = new BigDecimal(String.valueOf(tileResult.get(i)._2._1))
      val lat = new BigDecimal(String.valueOf(tileResult.get(i)._2._2))
      val cellsize = new BigDecimal(String.valueOf(tileResult.get(i)._3))
      val length =  new BigDecimal(String.valueOf(tileResult.get(i)._4._1))
      val width =  new BigDecimal(String.valueOf(tileResult.get(i)._4._2))

      val lng_Range = (lng,lng.add(cellsize.multiply(length)))
      val lat_Range = (lat,lat.add(cellsize.multiply(width)))
      //val geohashcode = new GeoHashTransform().encode(lat, lng)
      val number = tileResult.get(i)._1
     // println(lng_Range,lat_Range,number)
      tileRange.add((lng_Range,lat_Range,(tileResult.get(i)._4._1,tileResult.get(i)._4._2),number))
    }
    tileRange
  }
  def getLatAndLngByAddress(addr: String) = {
    var address = ""
    var lat = ""
    var lng = ""
    try{
      address = java.net.URLEncoder.encode(addr, "UTF-8")
    }
    catch {
      case e1: UnsupportedEncodingException => e1.printStackTrace()
    }
    val url = String.format("http://api.map.baidu.com/geocoder/v2/?" + "ak=uNuXZysRABMfumGLKtxwiMNsiN1dHGox&output=json&address=%s", address)
    //进行转码
    try{
      val myURL = new URL(url)
      val httpsConn:HttpURLConnection = myURL.openConnection.asInstanceOf[HttpURLConnection]
      if (httpsConn != null) {
        val insr = new InputStreamReader(httpsConn.getInputStream, "UTF-8")
        val br = new BufferedReader(insr)
        var data = ""
        if ((data = br.readLine) != null) {
          //println(data)
          lat = data.substring(data.indexOf("\"lat\":") + "\"lat\":".length, data.indexOf("},\"precise\""))
          lng = data.substring(data.indexOf("\"lng\":") + "\"lng\":".length, data.indexOf(",\"lat\""))
        }
        insr.close
      }
    } catch {
      case e: IOException =>e.printStackTrace()
    }
    val map = new util.HashMap[String,BigDecimal]
    map.put("lat",  new BigDecimal(lat))
    map.put("lng", new BigDecimal(lng))
    map
  }
  def search(tileRange: ArrayList[((BigDecimal, BigDecimal),(BigDecimal, BigDecimal),(Int,Int ),Int)], lng: String, lat: String) ={
    //遍历tileRange
    val lg = new BigDecimal(lng)
    val lt = new BigDecimal(lat)
    val last = tileRange.size
    var number = 0
    for (i <- Range(0, tileRange.size())) {
      val RangeData = tileRange.get(i)
      var a = lg.compareTo(RangeData._1._1)
      var b = lg.compareTo(RangeData._1._2)
      if(a != -1 && b != 1){
        var c = lt.compareTo(RangeData._2._1)
        var d = lt.compareTo(RangeData._2._2)
        if(c != -1 && d != 1){
          println("该瓦片lng范围为："+RangeData._1)
          println("该瓦片lat范围为："+RangeData._2)
          println("瓦片编号的hilbert值为："+RangeData._4)
          number = RangeData._4
        } else{
          if(i == last){println("没有找到，超出当前瓦片范围！")}
        }
      } else{
        if(i == last && number == 0){println("没有找到，超出当前瓦片范围！")}
      }
    }
    number
  }
  /*
  *@Param 要查找的左下的经纬度和右上的经纬度
  *@Return 返回该范围所有的hilbert的List
  *@Author:LiumingYan
  */
  def getAllNumber(left_down:(String,String),right_up:(String,String),tileList:ArrayList[(Int, (Double, Double), Double, (Int, Int), (Int, Int))]) = {
        val lng_range = (left_down._1.toDouble, right_up._1.toDouble)
        val lat_range = (left_down._2.toDouble, right_up._2.toDouble)
        val result_list = new util.ArrayList[Int]
        val list = tileFindRange(getLnglatofEachTile(tileList))
        for (i <- Range(0,list.size())) { //判断是否有交集
          if(lng_range._1<list.get(i)._1._2.doubleValue()&&lng_range._2>list.get(i)._1._1.doubleValue()&&
            lat_range._2>list.get(i)._2._1.doubleValue()&&list.get(i)._2._2.doubleValue()>lat_range._1){
            result_list.add(list.get(i)._4)
            println(list.get(i)._4)
          }
        }
        result_list
      }


//    val left_down_number = search((tileFindRange(getLnglatofEachTile(tileList))), left_down._1, left_down._2)
//    val right_up_number = search((tileFindRange(getLnglatofEachTile(tileList))), right_up._1, right_up._2)
//    var hilbertN = 0
//    val rangeList = new ArrayList[Int]
//    //判断该经纬度范围是否跨瓦片
//    if (left_down_number == right_up_number) {
//      //同一瓦片范围内
//      println(left_down_number)
//      rangeList.add(left_down_number)
//    } else {
//      val hilbert = new HilbertTransform
//      hilbertN = hilbert.rowNumAndColNumGetN(tileList.get(0)._5._1, tileList.get(0)._5._2)
//      val left_down = hilbert.hilbertCodeToXY(hilbertN, left_down_number)
//      //左下坐标
//      val right_up = hilbert.hilbertCodeToXY(hilbertN, right_up_number) //右上坐标
//      println(left_down, right_up)
//      for (j <- Range(left_down._1, right_up._1 + 1)) {
//        for (i <- Range(right_up._2, left_down._2 + 1)) {
//           println(i,j)
//          //println(hilbert.xyToHilbertCode(hilbertN,i,j))
//          rangeList.add(hilbert.xyToHilbertCode(hilbertN, i, j))
//        }
//      }
//    }
//    rangeList
//  }
  def transformDate(date:String) ={
    val a = date.split(" ")
    val b = a(0).split("-")
    val c = a(1).split(":")
    var Date = ""
    for(i <- Range(0,b.length)){
      Date = Date + b(i)
    }
    Date = Date + c(0)
    Date.toLong
  }
}

