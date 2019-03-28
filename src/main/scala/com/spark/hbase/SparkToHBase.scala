package com.spark.hbase

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util

import com.data.database.HBaseClient
import com.data.storage.GridDataToHBase
import com.spark.config.SparkConfig
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
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
  val propertyColumnsName = Array("x", "y", "lng", "lat", "cellSize", "hight", "width")

  val SparkConfig = new SparkConfig()
  val sc = SparkConfig.getSparkContext("local")


  def getRddByScanTable(tableName: String, startRowKey: Array[Byte], stopRowKey: Array[Byte], fc: Array[(String, Array[String])])={

    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val scan = new Scan()

    scan.setStartRow(startRowKey).setStopRow(stopRowKey)

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


  def printHashMap(map: util.HashMap[String, Array[Byte]] ): Unit ={
    val keysIterator = map.keySet().iterator()
    while (keysIterator.hasNext){
      val key = keysIterator.next()
      println(key + ": " + map.get(key))
    }

  }


  def main(args: Array[String]): Unit = {

    val fc = Array(("aqi", Array("data")), ("pm25", Array("data")), ("properties", Array("lng", "lat", "cellSize", "x", "y", "hight", "width")))
    val date = "2016-06-12 08:00:00"
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d = format.parse(date)
    val timestamp = new Timestamp(d.getTime()).getTime/1000
    val gridDataToHBase = new GridDataToHBase()
    val startRowKey = gridDataToHBase.generateRowKeyNoHilbertCode(timestamp, 1, 1)
    val stopRowKey = gridDataToHBase.generateRowKeyNoHilbertCode(timestamp, 1, 2)
    println(startRowKey)
    println(stopRowKey)

    val tableRdd = getRddByScanTable("gridAirData1", Bytes.toBytes(startRowKey), Bytes.toBytes(stopRowKey), fc)

    println(tableRdd.count())

    val propertyMap = getPropertiesFromTableRdd(tableRdd)
    printHashMap(propertyMap)

  }


}
