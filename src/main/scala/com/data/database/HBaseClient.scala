package com.data.database

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Get, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes

import scala.reflect.internal.util.TableDef.Column

object HBaseClient {

  val conf = HBaseConfiguration.create()


  def selectOneColumnByRowKey(tableName: String, rowKey: Array[Byte], family: Array[Byte], column: Array[Byte]): Array[Byte] ={
    val table = new HTable(conf, tableName)
    val get = new Get(rowKey)
    get.addColumn(family, column)
    val result = table.get(get)
    val value = result.getValue(family, column)
    value
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
