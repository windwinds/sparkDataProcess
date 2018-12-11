package com.data.database

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes

object HBaseClient {

  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()

    conf.set("hbase.zookeeper.quorum", "master, dell4, xiadclinux")

    val table = new HTable(conf, "testtable")

    val put = new Put(Bytes.toBytes("row1"))

    put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"))
    put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), Bytes.toBytes("val2"))

    table.put(put)

  }

}
