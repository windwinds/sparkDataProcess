package com.data.storage

import java.util
import java.util.ArrayList

import com.data.database.HBaseClient
import com.spark.utils.DataTransformUtil
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.util.Bytes



/**
  * @Auther: liyongchang
  * @Date: 2018/12/13 11:04
  * @Description:
  */
class GridDataToHBase {
  val tableName = "gridAirData3"
  val tableFamilies = Array("aqi", "pm25", "pm10", "co", "no2", "ozone1hour", "ozone8hour", "so2", "properties")
  HBaseClient.create_table(tableName, tableFamilies)

  val hTable = HBaseClient.getHTableByName(tableName)


  /**
    * 一次插入多条瓦片记录进入HBase, 将不同污染物的tileData合并同时插入数据库，速度更快
    * @param timeStamp
    * @param provinceNo
    * @param cityNo
    * @param tileDataList
    * @param columnFamilyList
    */
  def addData(timeStamp: Long, provinceNo: Long, cityNo: Long,
              tileDataList: ArrayList[(Int, ArrayList[Array[Float]], (Double, Double), Double, (Int, Int), (Int, Int))], columnFamilyList: ArrayList[String])={

    val putList = new util.ArrayList[Put]()
    for (i <- Range(0, tileDataList.size())){

      val tileData = tileDataList.get(i)
      val hilbertCode = tileData._1
      val rowKey = generateRowKey(timeStamp, provinceNo, cityNo, hilbertCode)
      val dataList = tileData._2
      val lnglat = tileData._3
      val cellSize = tileData._4
      val hightWidth = tileData._5

      //横纵轴瓦片的个数
      val rowTileNum = tileData._6._1
      val colTileNum = tileData._6._2

      val put = new Put(Bytes.toBytes(rowKey))
      //println(hilbertCode + ":" + hightWidth )

      put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("hight"), Bytes.toBytes(hightWidth._1))
      put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("width"), Bytes.toBytes(hightWidth._2))

      //只在编号为0的瓦片记录行中存储，节省空间
      if (hilbertCode == 0){
        put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("x"), Bytes.toBytes(rowTileNum))
        put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("y"), Bytes.toBytes(colTileNum))
        put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("lng"), Bytes.toBytes(lnglat._1))
        put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("lat"), Bytes.toBytes(lnglat._2))
        put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("cellSize"), Bytes.toBytes(cellSize))
        println("x: " + rowTileNum + "  y: " + colTileNum + " lng: " + lnglat._1 + " lat: " + lnglat._2 + " cellSize: " + cellSize)
      }

      for (pollutionIndex <- Range(0, columnFamilyList.size())){
        val data = dataList.get(pollutionIndex)
        val dataBytes = DataTransformUtil.floatArrayToBytesArray(data)
        put.addColumn(Bytes.toBytes(columnFamilyList.get(pollutionIndex)), Bytes.toBytes("data"), dataBytes)
      }
      putList.add(put)
    }
    hTable.put(putList)
  }


  /**
    * 生成表的rowKey
    * @param timeStamp:19-10位
    * @param provinceNo：9-8位
    * @param cityNo：7-6位
    * @param hilbertCode：5-1位
    * @return
    */
  def generateRowKey(timeStamp: Long, provinceNo: Long, cityNo: Long, hilbertCode: Long):Long={
    if (hilbertCode > 99999){
      println("瓦片数目过多，请增大瓦片大小")
      sys.exit()
    }
    timeStamp*1000000000L + provinceNo*10000000L + cityNo*100000L + hilbertCode
  }


  def generateRowKeyNoHilbertCode(timeStamp: Long, provinceNo: Long, cityNo: Long): Long ={
    val rowKey = timeStamp*1000000000L + provinceNo*10000000L + cityNo*100000L
    rowKey
  }
  /*
   *@Description:在表中查找对应瓦片
   *@Return:各污染物的瓦片数据
   *@Author:LiumingYan
   */
  def searchData(timeStamp: Long, provinceNo: Long, cityNo: Long,hilbertCode:Int,family: ArrayList[String],column: Array[Byte]) ={
    val rowKey = generateRowKey(timeStamp, provinceNo, cityNo, hilbertCode)
    var data = new scala.collection.mutable.HashMap[String,Array[Float]]()
    for (pollutionIndex <- Range(0, family.size())){
      val data_Byte = HBaseClient.selectOneColumnByRowKey(tableName, Bytes.toBytes(rowKey),  Bytes.toBytes(family.get(pollutionIndex)), column: Array[Byte])
      println((family.get(pollutionIndex),DataTransformUtil.bytesArrayToFloatArray(data_Byte)))
      data += (family.get(pollutionIndex) -> DataTransformUtil.bytesArrayToFloatArray(data_Byte))
    }
    data
  }
  //遍历表，获取Hilbert的最大值
  def getMaxHilbertCode(timeStamp: Long, provinceNo: Long, cityNo: Long) ={
    var max = 0
    val results = hTable.getScanner(new Scan())
    val it:util.Iterator[Result] = results.iterator()
    while(it.hasNext){
      val next = it.next()
      for(kv <- next.raw()){
        val rowkey =Bytes.toLong(kv.getRow()).toInt
        val hilbertCode = rowkey - (timeStamp * 1000000000L + provinceNo * 10000000L + cityNo * 100000L).toInt
        if(max > hilbertCode){
          max = max
        }else{
          max = hilbertCode
        }
      }
    }
    max
  }
  //获取各瓦片信息，存到list中
  def getAllTileList(timeStamp: Long, provinceNo: Long, cityNo: Long,max_hilbertCode:Long ) ={
    var rowNumber,colNumber,hight,width = 0
    var lng,lat,cellsize =0.000000
    val list = new ArrayList[(Int,(Double, Double), Double, (Int, Int), (Int, Int))]
    for(i <-Range(0,max_hilbertCode.toInt + 1)){
     val rowkey = generateRowKey(timeStamp,provinceNo,cityNo,i.toLong)
      if( i == 0 ){
        val name = Array("x", "y", "lng", "lat", "cellSize","hight","width")
        for( n <- name){
          val value = HBaseClient.selectOneColumnByRowKey("gridAirData3",Bytes.toBytes(rowkey), Bytes.toBytes("properties"), Bytes.toBytes(n))
            n match{
              case "x" => rowNumber = Bytes.toInt(value)
              case "y" => colNumber = Bytes.toInt(value)
              case "lng" => lng =  Bytes.toDouble(value)
              case "lat" => lat =  Bytes.toDouble(value)
              case "cellSize" => cellsize =  Bytes.toDouble(value)
              case "hight" => hight = Bytes.toInt(value)
              case "width" => width = Bytes.toInt(value)
              case _ => println("other")
            }
          }
        //println(i,(lng,lat),cellsize,(hight,width),(rowNumber,colNumber))
        list.add(i,(lng,lat),cellsize,(hight,width),(rowNumber,colNumber))
        }
      else{
        val value1 = HBaseClient.selectOneColumnByRowKey("gridAirData3",Bytes.toBytes(rowkey), Bytes.toBytes("properties"), Bytes.toBytes("hight"))
        val value2 = HBaseClient.selectOneColumnByRowKey("gridAirData3",Bytes.toBytes(rowkey), Bytes.toBytes("properties"), Bytes.toBytes("width"))
        if(value1 == null | value2 == null){
          //println(i + "is null")
        }else{
          hight = Bytes.toInt(value1)
          width = Bytes.toInt(value2)
          list.add(i,(lng,lat),cellsize,(hight,width),(rowNumber,colNumber))
          //println(i,(lng,lat),cellsize,(hight,width),(rowNumber,colNumber))
        }
    }
    }
    list
  }

  def tileToMap(list:ArrayList[(Int,(Double, Double), Double, (Int, Int), (Int, Int))])={
    var map = Map[Int,((Double, Double), Double, (Int, Int), (Int, Int))]()
    for(i <- Range(0,list.size())){
      map += (list.get(i)._1 -> (list.get(i)._2,list.get(i)._3,list.get(i)._4,list.get(i)._5))
    }
    map
  }




}
