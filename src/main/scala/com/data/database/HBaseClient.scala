package com.data.database

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.HashMap
import java.util.ArrayList

import com.data.storage.GridDataToHBase
import com.data.storage.ProvinceCityHbase.conf
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable
import scala.reflect.internal.util.TableDef.Column

object HBaseClient {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.rootdir", "hdfs://master:9000/hbase")
  conf.set("hbase.cluster.distributed", "true")
  conf.set("hbase.zookeeper.quorum", "master,dell4,xiadclinux")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.set("hbase.zookeeper.property.dataDir", "/usr/local/hadoop-2.6.0/zookeeper-3.4.8")
  conf.set("hbase.master.info.port", "60010")
  conf.set("hbase.regionserver.info.port", "60030")
  conf.set("hbase.rest.port", "8090")
  val connection = ConnectionFactory.createConnection(conf)

  val separator = ":"


  def getHTableByName(tableName: String)={
    connection.getTable(TableName.valueOf(tableName))
  }

  def create_table(table_name: String, colFamily: Array[String]){
    /*
    *@Description :创建hbase表
    *@Param 表名，列族
    *@Return 创建的hbase表
    *@Author:LiumingYan
    *@Date
    */
    //conf.set("hbase.zookeeper.quorum", "master, dell4, xiadclinux")
    val tableName = TableName.valueOf(table_name)
    val hadmin = connection.getAdmin()
    if (!hadmin.tableExists(tableName)) {
      println("Table Not Exists! Create Table")
      val tableDesc = new HTableDescriptor(table_name)
      for (str <- colFamily) {
        tableDesc.addFamily(new HColumnDescriptor(str))
      }
      hadmin.createTable(tableDesc)
    }
    else {
      println("Table  Exists!")
//      hadmin.disableTable(tableName)
//      hadmin.deleteTable(tableName)
//      println("Table  had deleted!")
//      val tableDesc = new HTableDescriptor(table_name)
//      for (str <- colFamily) {
//        tableDesc.addFamily(new HColumnDescriptor(str))
//      }
//      hadmin.createTable(tableDesc)
    }

  }


  def selectOneColumnByRowKey(tableName: String, rowKey: Array[Byte], family: Array[Byte], column: Array[Byte]): Array[Byte] ={
    val table = getHTableByName(tableName)
    val get = new Get(rowKey)
    get.addColumn(family, column)
    val result = table.get(get)
    val value = result.getValue(family, column)
    value
  }


  /**
    * 批量查询
    * @param tableName
    * @param rowKeys
    * @param family
    * @param column
    * 返回：[
    *  {
    *   row:{
    *       {family1:
    *           {column1:value1},
    *           {column2:value2},
    *       }
    *       {
    *       family2:
    *       }
    *     {
    *  }
    * ]
    */
  def selectMultiFcByRowKeys(tableName: String, rowKeys: Array[Array[Byte]], fc: Array[(String, Array[String])])={

    val htable = getHTableByName(tableName)

    val gets = new ArrayList[Get]()

    for (i <- Range(0, rowKeys.length)){

      val get = new Get(rowKeys(i))

      for (j <- Range(0, fc.length)){

        val family = fc(j)._1
        val columns = fc(j)._2

        val familyBytes = Bytes.toBytes(family)

        for (k <- Range(0, columns.length)){

          get.addColumn(familyBytes, Bytes.toBytes(columns(k)))

        }

      }
      gets.add(get)
    }

    val results = htable.get(gets)

    val resultList = getValueListFromResults(results, fc)
    resultList

  }


  def scanTable(tableName: String, startRowKey: Array[Byte], stopRowKey: Array[Byte], fc: Array[(String, Array[String])]):ArrayList[(Array[Byte], HashMap[String, Array[Byte]] )] ={

    val htable = getHTableByName(tableName)

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

    val selectResults = new ArrayList[(Array[Byte], HashMap[String, Array[Byte]] )]

    val resultScanner = htable.getScanner(scan)
    val it = resultScanner.iterator()
    while(it.hasNext){
      val result = it.next()
      val value = getValueFromResult(result, fc)
      selectResults.add( (value._1, value._2) )
    }

    resultScanner.close()

    selectResults
  }

  def getValueFromResult(result: Result, fc: Array[(String, Array[String])])={

    val row = result.getRow()
    val fcHashMap = new HashMap[String, Array[Byte]]
    for (j <- Range(0, fc.length)) {

      val family = fc(j)._1
      val columns = fc(j)._2

      val familyBytes = Bytes.toBytes(family)

      for (k <- Range(0, columns.length)) {

        val value = result.getValue(familyBytes, Bytes.toBytes(columns(k)))

        fcHashMap.put(family + separator + columns(k), value)

      }
    }
    (row, fcHashMap)
  }


  def getValueListFromResults(results: Array[Result], fc: Array[(String, Array[String])])={
    //ArrayList[(rowkey, ArrayList[(family, column, value)])]
    val selectResults = new ArrayList[(Array[Byte], HashMap[String, Array[Byte]] )]

    for (result <- results){

      val row = result.getRow()
      val fcHashMap = new HashMap[String, Array[Byte]]
      for (j <- Range(0, fc.length)) {

        val family = fc(j)._1
        val columns = fc(j)._2

        val familyBytes = Bytes.toBytes(family)

        for (k <- Range(0, columns.length)) {

          val value = result.getValue(familyBytes, Bytes.toBytes(columns(k)))

          fcHashMap.put(family + separator + columns(k), value)

        }
      }
      selectResults.add((row, fcHashMap))
    }
    selectResults

  }


  def main(args: Array[String]): Unit = {


    //conf.set("hbase.zookeeper.quorum", "master, dell4, xiadclinux")

//    val table = new HTable(conf, "testtable")
//
//    val put = new Put(Bytes.toBytes("row1"))
//
//    put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"))
//    put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val2"))
//
//    table.put(put)
    //val value = selectOneColumnByRowKey("testtable", Bytes.toBytes("row3"), Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"))
    //print(Bytes.toString(value))

    val rows = Array(Bytes.toBytes("row1"), Bytes.toBytes("row2"), Bytes.toBytes("row3"))

    val fc = Array(("aqi", Array("data")), ("pm25", Array("data")), ("properties", Array("lng", "lat", "cellSize")))

    val resultList = selectMultiFcByRowKeys("gridAirData", rows, fc)

    //println(resultList)

    val date = "2016-06-12 08:00:00"
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val d = format.parse(date)
    val timestamp = new Timestamp(d.getTime()).getTime/1000

    val gridDataToHBase = new GridDataToHBase()

    val startRowKey = gridDataToHBase.generateRowKeyNoHilbertCode(timestamp, 1, 1)
    val stopRowKey = gridDataToHBase.generateRowKeyNoHilbertCode(timestamp, 1, 2)

    val result = scanTable("gridAirData", Bytes.toBytes(startRowKey), Bytes.toBytes(stopRowKey), fc)
    println(result)


  }

}
