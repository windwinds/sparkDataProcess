package com.data.storage

import java.util
import java.util.ArrayList

import com.data.database.HBaseClient
import com.spark.utils.DataTransformUtil
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

/**
  * @Auther: liyongchang
  * @Date: 2018/12/13 11:04
  * @Description:
  */
class GridDataToHBase {

  //create 'gridAirData', 'aqi', 'pm25', 'pm10', 'co', 'no2', 'ozone1hour', 'ozone8hour', 'so2', 'properties'
  val tableName = "gridAirDataWithProperties_region1G_ALL"
  val tableFamilies = Array("aqi", "pm25", "pm10", "co", "no2", "so2", "properties")
  HBaseClient.create_table(tableName, tableFamilies)

  val hTable = HBaseClient.getHTableByName(tableName)

  val proTableName = "ascProperty"
  val proFamilies = Array("properties")
  HBaseClient.create_table(proTableName, proFamilies)
  val proTable = HBaseClient.getHTableByName(proTableName)

  /**
    * 一次插入多条瓦片记录进入HBase, 将不同污染物的tileData合并同时插入数据库，速度更快
    * @param timeStamp
    * @param provinceNo
    * @param cityNo
    * @param tileDataList
    * @param columnFamilyList
    */
  def addData(timeStamp: Long, provinceNo: Long, cityNo: Long,
              tileDataList: ArrayList[(Int, ArrayList[Array[Float]], (Double, Double), Double, (Int, Int), (Int, Int), Double)], columnFamilyList: ArrayList[String])={

    val putList = new util.ArrayList[Put]()
    for (i <- Range(0, tileDataList.size())){

      val tileData = tileDataList.get(i)
      val hilbertCode = tileData._1
      val rowKey = generateRowKey(timeStamp, provinceNo, cityNo, hilbertCode)
      val dataList = tileData._2
      val lnglat = tileData._3
      val cellSize = tileData._4
      val hightWidth = tileData._5

      val noData = tileData._7
      //横纵轴瓦片的个数
      val rowTileNum = tileData._6._1
      val colTileNum = tileData._6._2

      val put = new Put(Bytes.toBytes(rowKey))
      //当前瓦片像元高度,不同时间，相同区域的瓦片元数据信息相同，因此不需要设置时间戳
      put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("hight"), timeStamp, Bytes.toBytes(hightWidth._1))
      //当前瓦片像元宽度
      put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("width"), timeStamp, Bytes.toBytes(hightWidth._2))

      //只在编号为0的瓦片记录行中存储，节省空间
//      if (hilbertCode == 0){
//        //横轴方向瓦片个数
//        put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("x"), timeStamp, Bytes.toBytes(rowTileNum))
//        //纵轴方向瓦片个数
//        put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("y"), timeStamp, Bytes.toBytes(colTileNum))
//        //栅格数据左下角经度
//        put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("lng"), timeStamp, Bytes.toBytes(lnglat._1))
//        //栅格数据左下角纬度
//        put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("lat"), timeStamp, Bytes.toBytes(lnglat._2))
//        //像元大小
//        put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("cellSize"), timeStamp, Bytes.toBytes(cellSize))
//        //println("x: " + rowTileNum + "  y: " + colTileNum + " lng: " + lnglat._1 + " lat: " + lnglat._2 + " cellSize: " + cellSize)
//        //无效值
//        put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("noData"), timeStamp, Bytes.toBytes(noData))
//      }
      //横轴方向瓦片个数
      put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("x"), timeStamp, Bytes.toBytes(rowTileNum))
      //纵轴方向瓦片个数
      put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("y"), timeStamp, Bytes.toBytes(colTileNum))
      //栅格数据左下角经度
      put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("lng"), timeStamp, Bytes.toBytes(lnglat._1))
      //栅格数据左下角纬度
      put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("lat"), timeStamp, Bytes.toBytes(lnglat._2))
      //像元大小
      put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("cellSize"), timeStamp, Bytes.toBytes(cellSize))
      //println("x: " + rowTileNum + "  y: " + colTileNum + " lng: " + lnglat._1 + " lat: " + lnglat._2 + " cellSize: " + cellSize)
      //无效值
      put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("noData"), timeStamp, Bytes.toBytes(noData))

      for (pollutionIndex <- Range(0, columnFamilyList.size())){
        val data = dataList.get(pollutionIndex)
        val dataBytes = DataTransformUtil.floatArrayToBytesArray(data)
        //通过设置version来区别不同时间的瓦片数据
        put.addColumn(Bytes.toBytes(columnFamilyList.get(pollutionIndex)), Bytes.toBytes("data"), timeStamp,dataBytes)
      }
      putList.add(put)
    }
    hTable.put(putList)
  }

  /**
    * 插入元数据信息
    */
  def insertProperties(provinceNo: Long, cityNo: Long, propertiesAsc: (Int, Int, Double, Double, Double, Double), tileLength: Int)={

    val rowKey = provinceNo*100L + cityNo
    val put = new Put(Bytes.toBytes(rowKey))
    val timeStamp = 1
    put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("ncols"), timeStamp, Bytes.toBytes(propertiesAsc._1))
    put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("nrows"), timeStamp, Bytes.toBytes(propertiesAsc._2))
    put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("xllcorner"), timeStamp, Bytes.toBytes(propertiesAsc._3))
    put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("yllcorner"), timeStamp, Bytes.toBytes(propertiesAsc._4))
    put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("cellsize"), timeStamp, Bytes.toBytes(propertiesAsc._5))
    put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("NODATA_value"), timeStamp, Bytes.toBytes(propertiesAsc._6))

    put.addColumn(Bytes.toBytes("properties"), Bytes.toBytes("tileLength"), timeStamp, Bytes.toBytes(tileLength))

    proTable.put(put)
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
    //timeStamp*1000000000L + provinceNo*10000000L + cityNo*100000L + hilbertCode
    provinceNo*10000000L + cityNo*100000L + hilbertCode
  }


  def generateRowKeyNoHilbertCode(provinceNo: Long, cityNo: Long): Long ={
    //val rowKey = timeStamp*1000000000L + provinceNo*10000000L + cityNo*100000L
    val rowKey = provinceNo*10000000L + cityNo*100000L
    rowKey
  }




}
