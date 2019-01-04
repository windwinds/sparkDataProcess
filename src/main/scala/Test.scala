import org.apache.spark.{SparkConf, SparkContext}
import java.io.File
import java.sql.{Connection, DriverManager, ResultSet}

import org.apache.hadoop.hbase.{Cell, CellUtil}
import org.apache.hadoop.hbase.client.{ResultScanner, Scan}
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import com.owlike.genson.defaultGenson._
import org.apache.commons.io.FileUtils
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}
import org.apache.hadoop.hbase.client.{Get, HBaseAdmin, HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
/**
  * @Auther: liyongchang
  * @Date: 2018/10/31 15:25
  * @Description
  */
object Test {
  var hprovince, hcity, hcode, province_code = ""

  //spark运行环境
  val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("test"))

  //hbase运行环境
  val conf = HBaseConfiguration.create()
  conf.set("hbase.rootdir", "hdfs://master:9000/hbase")
  conf.set("hbase.cluster.distributed", "true")
  conf.set("hbase.zookeeper.quorum", "master,dell4,xiadclinux")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.set("hbase.zookeeper.property.dataDir", "/usr/local/hadoop-2.6.0/zookeeper-3.4.8")
  conf.set("hbase.master.info.port", "60010")
  conf.set("hbase.regionserver.info.port", "60030")
  conf.set("hbase.rest.port", "8090")

  //  //设置查询的表名
  //  conf.set(TableInputFormat.INPUT_TABLE, "user")

  case class city(province: String, code: String, cities: List[city_result])

  case class city_result(name: String, code: String)

  def create_table(table_name: String, colFamily: Array[String]): HTable = {
    //conf.set("hbase.zookeeper.quorum", "master, dell4, xiadclinux")
    val hadmin = new HBaseAdmin(conf)
    if (!hadmin.isTableAvailable(table_name)) {
      print("Table Not Exists! Create Table")
      val tableDesc = new HTableDescriptor(table_name)
      for (str <- colFamily) {
        tableDesc.addFamily(new HColumnDescriptor(str))
      }
      hadmin.createTable(tableDesc)
    } else {
      print("Table  Exists!")
      hadmin.disableTable(table_name)
      hadmin.deleteTable(table_name)
      print("Table  had deleted!")
      val tableDesc = new HTableDescriptor(table_name)
      for (str <- colFamily) {
        tableDesc.addFamily(new HColumnDescriptor(str))
      }
      hadmin.createTable(tableDesc)
    }
    val table = new HTable(conf, table_name)
    table
  }

  def insertData(hcode: String, hprovince: String, hcity: String,site:String,table: HTable): Unit = {
    val put: Put = new Put(Bytes.toBytes(hcode))
    put.add(Bytes.toBytes("cf1"), Bytes.toBytes("c1"), Bytes.toBytes(hprovince))
    put.add(Bytes.toBytes("cf1"), Bytes.toBytes("c2"), Bytes.toBytes(hcity))

    put.add(Bytes.toBytes("site"), Bytes.toBytes(""), Bytes.toBytes(site))
    table.put(put)
  }

  def selectOneColumnByRowKey(table: HTable, rowKey: Array[Byte], family: Array[Byte], column: Array[Byte]): Array[Byte] = {
    val get = new Get(rowKey)
    get.addColumn(family, column)
    val result = table.get(get)
    val value = result.getValue(family, column)
    value
  }

//  def scan(table: HTable, rowKey: Array[Byte], family: Array[Byte], column: Array[Byte]): Unit = {
//    val s = new Scan()
//    s.addColumn(family, column)
//    val scanner:ResultScanner = table.getScanner(s)
//    val results = scanner.iterator
//    try {
////      for (r <- scanner) {
////        println("Found row: " + r)
////        println("Found value: " + Bytes.toString(
////          r.getValue(family, column)))
////      }
//      while (results.hasNext()) {
//        val r = results.next()
//        val rowkey = Bytes.toString(r.getRow())
//        val province: Cell = r.getColumnLatestCell(Bytes.toBytes("cf1"), Bytes.toBytes("c1"))
//        val city: Cell = r.getColumnLatestCell(Bytes.toBytes("cf1"), Bytes.toBytes("c2"))
//        // Cell cAge = r.getColumnLatestCell(Bytes.toBytes("f1"),Bytes.toBytes("age"))
//        val hprovince = new String(CellUtil.cloneValue(province), "utf-8")
//        val hcity = new String(CellUtil.cloneValue(city), "utf-8")
//        System.out.println("-----------------------------")
//        System.out.println(rowkey + "," + hprovince + "," + hcity)
//      }
//
//    } finally {
//      //确保scanner关闭
//      scanner.close()
//    }
//  }
  def get_province_site(province:String ): ResultSet ={
    //连接mysql得到site
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.1.211:3306/geo_calculation_db?useUnicode=true&characterEncoding=utf8"
    val username = "root"
    val password = ""
    var connection:Connection = null
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      val statement = connection.createStatement()
      val sql ="select * from MAP_PROVINCE_TBL where name="+"'"+province+"'"
      val resultSet = statement.executeQuery(sql)
//  if(resultSet.next()) {
//    println("ResultSet is not null!")
//  }else{
//    println("ResultSet is null!")
//  }
    resultSet
  }
  def get_city_site(hcity:String ): ResultSet ={
    //连接mysql得到site
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.1.211:3306/geo_calculation_db?useUnicode=true&characterEncoding=utf8"
    val username = "root"
    val password = ""
    var connection:Connection = null

    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)

    val statement = connection.createStatement()
    val resultSet = statement.executeQuery("select * from MAP_CITY_TBL_ where name=" +"'"+ hcity+"'")

//    connection.close()
    resultSet
  }

  //    //利用spark 读取hbase表格
  //  def scan(table:HTable,rowkey:String ): Unit ={
  //    val hbaseRDD = sc.newAPIHadoopRDD(conf,classOf[TableInputFormat],classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
  //      classOf[org.apache.hadoop.hbase.client.Result])
  //    val count = hbaseRDD.count()
  //    hbaseRDD.cache()
  //    val g =new Get(rowkey.getBytes)
  //    val result = table.get(g)
  //    val value = new String (result.getValue("cf1".getBytes,"c2".getBytes),"utf-8")
  //
  //    val res = hbaseRDD.take(count.toInt)
  //    for(j <- 1 until count.toInt){
  //      var rs = res(j-1)._2
  //      var kvs = rs.raw()
  //      for(kv <- kvs){
  //        println ("rowkey:" + new String(kv.getRow) )+
  //        "cf:" + new String(kv.getFamily)+
  //        "column:" + new String (kv.getQualifierArray())
  //        "value" + new String (kv.getValue)
  //      }
  //    }
  //  }

  //将json文件转为对象数组
  def read_json(inputPath: String): List[city] = {
    val allCity = fromJson[List[city]](FileUtils.readFileToString(new File(inputPath), "UTF-8"))
    allCity.foreach(
      {
        r =>
          r match {
            case city => println(city)
            case other => println("unknown data structure" + other)
          }
      }
    )
    allCity
  }

  def main(args: Array[String]): Unit = {
    //json文件处理
    val inputPath = "C:/Users/109/Desktop/ChinaCity/ChinaCityList.json"
    val allCity = read_json(inputPath)
    val colFamily = Array("cf1","site")
    val table = create_table("province_city", colFamily)

    //插入全国数据
    insertData("0000", "中国", "中国","108.5525,34,3227",table)
    for (city <- allCity) {
      //插入省级数据
      hprovince = city.province
      province_code = city.code
      val resultSet = get_province_site(hprovince)
      resultSet.first()
      val site = resultSet.getString("cp")
      insertData(province_code, hprovince, hprovince, site, table)
      println(hprovince+":site = " + site)

      //插入省市数据
      for (city_result <- city.cities) {
        hcity = city_result.name.toString
        hcode = city_result.code.toString
        val resultSet_city = get_city_site(hcity: String)
        if (resultSet_city.next()) {
          resultSet_city.first()
          val site_city = resultSet_city.getString("cp")
          println(hcity+":sit_city = " + site_city)
          insertData(hcode, hprovince, hcity, site_city, table)

          //insertData(hcode, hprovince, hcity, table)
        }else{
          println("Don not have" + hcity + "site")
        }
      }

    }
    val s = selectOneColumnByRowKey(table, Bytes.toBytes("2204"), Bytes.toBytes("cf1"), Bytes.toBytes("c2"))
    val c = selectOneColumnByRowKey(table, Bytes.toBytes("2204"), Bytes.toBytes("cf1"), Bytes.toBytes("c1"))
    println(new String(s, "utf-8"))
    println(new String(c, "utf-8"))
  }
}


