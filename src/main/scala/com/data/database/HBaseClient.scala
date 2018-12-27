package com.data.database

import java.util.ArrayList

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes

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


  def getHTableByName(tableName: String)={
    connection.getTable(TableName.valueOf(tableName))
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
  def selectMultiFcByRowKeys(tableName: String, rowKeys: ArrayList[Array[Byte]], fc: ArrayList[(String, ArrayList[String])])={

    val htable = getHTableByName(tableName)

    val gets = new ArrayList[Get]()

    for (i <- Range(0, rowKeys.size())){

      val get = new Get(rowKeys.get(i))

      for (j <- Range(0, fc.size())){

        val family = fc.get(j)._1
        val columns = fc.get(j)._2

        val familyBytes = Bytes.toBytes(family)

        for (k <- Range(0, columns.size())){

          get.addColumn(familyBytes, Bytes.toBytes(columns.get(k)))

        }

      }
      gets.add(get)
    }

    val results = htable.get(gets)

    //ArrayList[(rowkey, ArrayList[(family, column, value)])]
    val selectResults = new ArrayList[(Array[Byte], ArrayList[(String, String, Array[Byte])])]

    for (result <- results){

      val row = result.getRow()

      for (j <- Range(0, fc.size())) {

        val family = fc.get(j)._1
        val columns = fc.get(j)._2

        val familyBytes = Bytes.toBytes(family)
        val arrayFc = new ArrayList[(String, String, Array[Byte])]
        for (k <- Range(0, columns.size())) {

          val value = result.getValue(familyBytes, Bytes.toBytes(columns.get(k)))

          arrayFc.add(family, columns.get(k), value)

        }
        selectResults.add((row, arrayFc))
      }
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
    val value = selectOneColumnByRowKey("testtable", Bytes.toBytes("row1"), Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"))
    print(Bytes.toString(value))

  }

}
