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
  val tableName = "gridAirData"

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

      val put = new Put(Bytes.toBytes(rowKey))
      put.add(Bytes.toBytes("properties"), Bytes.toBytes("lng"), Bytes.toBytes(lnglat._1))
      put.add(Bytes.toBytes("properties"), Bytes.toBytes("lat"), Bytes.toBytes(lnglat._2))
      put.add(Bytes.toBytes("properties"), Bytes.toBytes("cellSize"), Bytes.toBytes(cellSize))
      put.add(Bytes.toBytes("properties"), Bytes.toBytes("hight"), Bytes.toBytes(hightWidth._1))
      put.add(Bytes.toBytes("properties"), Bytes.toBytes("width"), Bytes.toBytes(hightWidth._2))
      for (pollutionIndex <- Range(0, columnFamilyList.size())){
        val data = dataList.get(pollutionIndex)
        val dataBytes = DataTransformUtil.floatArrayToBytesArray(data)
        put.add(Bytes.toBytes(columnFamilyList.get(pollutionIndex)), Bytes.toBytes("data"), dataBytes)
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





}
