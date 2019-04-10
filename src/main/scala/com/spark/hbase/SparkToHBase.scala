package com.spark.hbase

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import com.data.database.HBaseClient
import com.data.database.HBaseClient.{getHTableByName, getValueFromResult}
import com.data.split.Test.getProvinceCityInfoFromHBase
import com.data.storage.GridDataToHBase
import com.spark.config.SparkConfig
import com.spark.solap.{AggregationFunction, SpatioTemporalQuery}
import com.spark.utils.DataTransformUtil
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD



/**
  * @Auther: liyongchang
  * @Date: 2019/1/3 11:36
  * @Description: spark 操作HBase数据库
  */
object SparkToHBase {

  //System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val conf = HBaseConfiguration.create()
  conf.set("hbase.rootdir", "hdfs://master:9000/hbase")
  conf.set("hbase.cluster.distributed", "true")
  conf.set("hbase.zookeeper.quorum", "master,dell4,xiadclinux")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.set("hbase.zookeeper.property.dataDir", "/usr/local/hadoop-2.6.0/zookeeper-3.4.8")
  conf.set("hbase.master.info.port", "60010")
  conf.set("hbase.regionserver.info.port", "60030")
  conf.set("hbase.rest.port", "8090")

  val propertyFamilyName = "properties"
  val propertyColumnsName = Array("x", "y", "lng", "lat", "cellSize", "hight", "width", "noData")


  def getRddByScanTable(sc: SparkContext, tableName: String, startRowKey: Array[Byte], stopRowKey: Array[Byte], startTime: Long, endTime: Long, fc: Array[(String, Array[String])])={

    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val scan = new Scan()

    scan.setStartRow(startRowKey).setStopRow(stopRowKey)

    scan.setMaxVersions()
    scan.setTimeRange(startTime, endTime)

    for (j <- Range(0, fc.length)){

      val family = fc(j)._1
      val columns = fc(j)._2

      val familyBytes = Bytes.toBytes(family)

      for (k <- Range(0, columns.length)){

        scan.addColumn(familyBytes, Bytes.toBytes(columns(k)))

      }
    }

    val proto = ProtobufUtil.toScan(scan)
    val scanToString = Base64.encodeBytes(proto.toByteArray)
    conf.set(TableInputFormat.SCAN, scanToString)

    val rdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    rdd

  }


  /**
    * 从查询结果中的第一条记录中取出基本数据信息
    */
  def getPropertiesFromTableRdd(tableRdd: RDD[(ImmutableBytesWritable, Result)])={

    val firstResult = tableRdd.first()._2
    val familyBytes = Bytes.toBytes(propertyFamilyName)

    val propertyMap = new util.HashMap[String, Array[Byte]]

    for (column <- propertyColumnsName){

      val key = propertyFamilyName + HBaseClient.separator + column
      val value = firstResult.getValue(familyBytes, Bytes.toBytes(column))

      propertyMap.put(key, value)

    }

    propertyMap

  }

  /**
    * 从查询结果的tableRdd中获取瓦片像元数据和瓦片长宽
    * @param tableRdd
    */
  def getTileDataRddFromTableRdd(tableRdd: RDD[(ImmutableBytesWritable, Result)], tileDataFamily: String)={

    tableRdd.flatMap(x => {

      val result = x._2

      //println("listCells: " + result.listCells())

      val rowKey = result.getRow

      val propertyFamilyBytes = Bytes.toBytes(propertyFamilyName)
      val tileHight = result.getValue(propertyFamilyBytes, Bytes.toBytes("hight"))
      val tileWidth = result.getValue(propertyFamilyBytes, Bytes.toBytes("width"))
      //val noData = Bytes.toDouble(result.getValue(propertyFamilyBytes, Bytes.toBytes("noData")))

      val tileDateFamilyBytes = Bytes.toBytes(tileDataFamily)
      //val tileData = result.getValue(tileDateFamilyBytes, Bytes.toBytes("data"))
      //获得多个版本的数据 
      val dataKeyValueList = result.getColumn(tileDateFamilyBytes, Bytes.toBytes("data"))

      val array = new util.ArrayList[(Long, Long, Int, Int, Array[Float])]()

      for (i <- Range(0, dataKeyValueList.size())){
        val timestamp = dataKeyValueList.get(i).getTimestamp()
        val tileData = dataKeyValueList.get(i).getValue()
        array.add((Bytes.toLong(rowKey),timestamp, Bytes.toInt(tileHight), Bytes.toInt(tileWidth), DataTransformUtil.bytesArrayToFloatArray(tileData)))
      }
      val len = array.size()
      val arrayResult = new Array[(Long, Long, Int, Int, Array[Float])](len)
      for (i <- Range(0, len)){
        arrayResult(i) = array.get(i)
      }
      arrayResult

    })

  }


  def getTileDataWithPropertyRddFromTableRdd(tableRdd: RDD[(ImmutableBytesWritable, Result)], tileDataFamily: String)={

    tableRdd.flatMap(x => {

      val result = x._2

      //println("listCells: " + result.listCells())

      val rowKey = result.getRow

      val propertyFamilyBytes = Bytes.toBytes("properties")
      val lng = Bytes.toDouble(result.getValue(propertyFamilyBytes, Bytes.toBytes("lng")))
      val lat = Bytes.toDouble(result.getValue(propertyFamilyBytes, Bytes.toBytes("lat")))
      val tileHight = Bytes.toInt(result.getValue(propertyFamilyBytes, Bytes.toBytes("hight")))
      val tileWidth = Bytes.toInt(result.getValue(propertyFamilyBytes, Bytes.toBytes("width")))
      val xTileCount = Bytes.toInt(result.getValue(propertyFamilyBytes, Bytes.toBytes("x")))
      val yTileCount = Bytes.toInt(result.getValue(propertyFamilyBytes, Bytes.toBytes("y")))
      val cellSize = Bytes.toDouble(result.getValue(propertyFamilyBytes, Bytes.toBytes("cellSize")))
      val noData = Bytes.toDouble(result.getValue(propertyFamilyBytes, Bytes.toBytes("noData")))

      val properties = (xTileCount, yTileCount, lng, lat, cellSize, tileHight, tileWidth, noData)

      val tileDateFamilyBytes = Bytes.toBytes(tileDataFamily)
      //val tileData = result.getValue(tileDateFamilyBytes, Bytes.toBytes("data"))
      //获得多个版本的数据
      val dataKeyValueList = result.getColumn(tileDateFamilyBytes, Bytes.toBytes("data"))

      val array = new util.ArrayList[(Long, Long, (Int, Int, Double, Double, Double, Int, Int, Double), Array[Float])]()

      for (i <- Range(0, dataKeyValueList.size())){
        val timestamp = dataKeyValueList.get(i).getTimestamp()
        val tileData = dataKeyValueList.get(i).getValue()
        array.add((Bytes.toLong(rowKey),timestamp, properties, DataTransformUtil.bytesArrayToFloatArray(tileData)))
      }
      val len = array.size()
      val arrayResult = new Array[(Long, Long, (Int, Int, Double, Double, Double, Int, Int, Double), Array[Float])](len)
      for (i <- Range(0, len)){
        arrayResult(i) = array.get(i)
      }
      arrayResult

    })

  }


  def printHashMap(map: util.HashMap[String, Array[Byte]] ): Unit ={
    val keysIterator = map.keySet().iterator()
    while (keysIterator.hasNext){
      val key = keysIterator.next()
      println(key + ": " + map.get(key))
    }

  }

  /**
    * 将瓦片数据还原成原始栅格数据
    * @param args
    */
  def tileDataToGrid(tileRdd: RDD[(Long, Int, Int, Array[Float])])={



  }


  def dateToTimeStamp(date: String):Long={
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d = format.parse(date)
    val timestamp = new Timestamp(d.getTime()).getTime/1000
    timestamp
  }

  def getProvinceAndCityNoByName(provinceName:String, cityName:String)={

    val provinceCityMap = getProvinceCityInfoFromHBase()
    var provinceCityNo = ""
    if (provinceCityMap.get(provinceName + ":" + cityName) != null){
      provinceCityNo = provinceCityMap.get(provinceName + ":" + cityName)
    }else{
      provinceCityNo = provinceCityMap.get(provinceName + ":" + cityName + "市")
    }
    val provinceNo = provinceCityNo.substring(0, 2).toLong
    val cityNo = provinceCityNo.substring(2,4).toLong
    (provinceNo, cityNo)
  }


  /**
    * 将原始asc文件的元数据信息广播出去
    * @param args
    */
  def broadCastAscInfo(sc: SparkContext)={

    val resultMap = new util.HashMap[Long, (Int, Int, Double, Double,Double,Double, Int)]()

    val startRowKey = Bytes.toBytes(0000L)
    val endRowKey = Bytes.toBytes(9999L)

    val fc = Array(("properties", Array("ncols", "nrows", "xllcorner", "yllcorner", "cellsize", "NODATA_value", "tileLength")))

    val selectResults = HBaseClient.scanTable("ascProperty", startRowKey, endRowKey, fc)

    for (i <- Range(0, selectResults.size())){

      val rowKey = Bytes.toLong(selectResults.get(i)._1)

      val hashMap = selectResults.get(i)._2

      val ncols = Bytes.toInt(hashMap.get("properties:ncols"))
      val nrows = Bytes.toInt(hashMap.get("properties:nrows"))
      val xllcorner = Bytes.toDouble(hashMap.get("properties:xllcorner"))
      val yllcorner = Bytes.toDouble(hashMap.get("properties:yllcorner"))
      val cellsize = Bytes.toDouble(hashMap.get("properties:cellsize"))
      val NODATA_value = Bytes.toDouble(hashMap.get("properties:NODATA_value"))

      val tileLength = Bytes.toInt(hashMap.get("properties:tileLength"))

      //println(rowKey + "  " + province + ":" + city + "  " + lnglat)
      resultMap.put(rowKey, (ncols, nrows, xllcorner, yllcorner, cellsize, NODATA_value, tileLength))
    }

    sc.broadcast(resultMap)

  }


  def select(tableName: String, startRowKey: Array[Byte], stopRowKey: Array[Byte], startTime: Long, endTime: Long, fc: Array[(String, Array[String])])={

    val scan = new Scan()

    scan.setStartRow(startRowKey).setStopRow(stopRowKey)

    scan.setMaxVersions()
    scan.setTimeRange(startTime, endTime)

    for (j <- Range(0, fc.length)){

      val family = fc(j)._1
      val columns = fc(j)._2

      val familyBytes = Bytes.toBytes(family)

      for (k <- Range(0, columns.length)){

        scan.addColumn(familyBytes, Bytes.toBytes(columns(k)))

      }
    }
    val htable = getHTableByName(tableName)

    val resultScanner = htable.getScanner(scan)
    val it = resultScanner.iterator()
    while(it.hasNext){
      val result = it.next()
    }
    resultScanner.close()

  }



  def main(args: Array[String]): Unit = {
    val SparkConfig = new SparkConfig()
    val sc = SparkConfig.getSparkContext("local")

    //val ascInfoBroadCast = broadCastAscInfo(sc)

    val fc = Array(("aqi", Array("data")), ("pm25", Array("data")),("pm10", Array("data")),("co", Array("data")),("no2", Array("data")),("so2", Array("data")),("properties", Array("lng", "lat", "cellSize", "x", "y", "hight", "width", "noData")))
    val startDate = "2015-01-01 00:00:00"
    val endDate = "2015-08-01 00:00:00"
    val startTimeStamp = dateToTimeStamp(startDate)
    val endTimeStamp = dateToTimeStamp(endDate)
    val gridDataToHBase = new GridDataToHBase()

    val province = "湖北"
    val city = "武汉"
    val provinceCity = getProvinceAndCityNoByName(province, city)
    val startRowKey = gridDataToHBase.generateRowKeyNoHilbertCode(provinceCity._1, provinceCity._2)
    val stopRowKey = gridDataToHBase.generateRowKeyNoHilbertCode(provinceCity._1, provinceCity._2 + 1)
    val startTime = System.currentTimeMillis()
    val tableRdd = getRddByScanTable(sc, "gridAirDataWithProperties_ALL", Bytes.toBytes(startRowKey), Bytes.toBytes(stopRowKey),startTimeStamp,endTimeStamp, fc)
    println(tableRdd.count())
    println(System.currentTimeMillis() - startTime)
    //
//    val startTime = System.currentTimeMillis()
//    select("gridAirDataWithProperties_ALL", Bytes.toBytes(startRowKey), Bytes.toBytes(stopRowKey),startTimeStamp,endTimeStamp, fc)
//    println(System.currentTimeMillis() - startTime)
    //val propertyMap = getPropertiesFromTableRdd(tableRdd)
    //printHashMap(propertyMap)

    //tableRdd.persist()

    //val pollutions = Array("aqi", "pm25", "pm10", "co", "no2", "so2")
//    val pollution = "pm25"
//    println(pollution + "---------------------------------------------------------------------------------")
//    val tileDataRdd = getTileDataWithPropertyRddFromTableRdd(tableRdd, pollution)
//    val spatioTemporalQuery = new SpatioTemporalQuery()
//    val result = spatioTemporalQuery.regularPatternInDay(tileDataRdd, "day")
//
//    result.foreach(println)

    //空间查询
//    val tileMapRdd = spatioTemporalQuery.spatialQuery(tileDataRdd, "")
//    val ascRdd = spatioTemporalQuery.tileToAscRaster(tileMapRdd, ascInfoBroadCast.value)
//    ascRdd.coalesce(1).map(kv=>kv._2).saveAsTextFile("result")


    //println(tileDataRdd.count())

    //val firstData = tileDataRdd.collect()
    //println(firstData)
    //firstData.foreach(println)
//
//    val aggregateFun = new AggregationFunction()
//    println(aggregateFun.tileAvg(tileDataRdd))
//
//    println(aggregateFun.tileMax(tileDataRdd))
//    println(aggregateFun.tileMin(tileDataRdd))





  }


}
