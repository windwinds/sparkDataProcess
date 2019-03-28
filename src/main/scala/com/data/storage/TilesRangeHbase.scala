package src.main.scala.com.data.storage


import java.util.ArrayList

import com.data.split.{GeoHashTransform, HilbertTransform}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks._
import scala.collection.JavaConversions._
import com.data.database.HBaseClient
import java.io.IOException

import com.data.storage.GridDataToHBase

import scala.util.control.Breaks
/*
*@Description: 计算每个瓦片的经纬度范围
*@Return:${returns}
*@Author:LiumingYan
*@Date 17:522019/1/11
*/
class TilesRangeHbase {
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

  conf.set("mapreduce.task.timeout","1200000 ")
  conf.set("hbase.client.scanner.timeout.period","600000")
  conf.set("hbase.rpc.timeout","600000")

  val hTable = HBaseClient.getHTableByName("TileRange1")

  def tileRangeHbase(time:String,province:String,city:String,tileResult: ArrayList[(Int, (Double, Double), Double, (Int, Int), (Int, Int))]) = {
    for (i <- Range(0, tileResult.size)){
      //计算每个瓦片的经纬度范围
      val lng = tileResult.get(i)._2._1
      val lat = tileResult.get(i)._2._2
      val geohashcode = new GeoHashTransform().encode(lat, lng)
      val number = tileResult.get(i)._1
      //添加GeoHash编码-瓦片编码数据
      val put: Put = new Put(Bytes.toBytes(generateRowKeyNogeohashCode(time,province,city)+"-"+geohashcode))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("tilenumber"), Bytes.toBytes(number))
      hTable.put(put)
    }
    hTable
  }
  def generateRowKey(time: String, pcNo: String , geohashCode: String)={
    val rowkey = time + "-" + pcNo + "-" + geohashCode
    rowkey
  }
  def generateRowKeyNogeohashCode(time: String, provinceNo: String, cityNo: String ) ={
    val rowKey = time + "-" + provinceNo + cityNo
    rowKey
  }
  def searchHbase(data:(String,Int)) ={
    val array = data._1.split("-")
    val rowkey = Bytes.toBytes(generateRowKey(array(0),array(1),array(2)))
    val value = HBaseClient.selectOneColumnByRowKey("TileRange1",rowkey,Bytes.toBytes("cf1"),Bytes.toBytes("tilenumber"))
    value
  }
  def similarityMatch(InputGeohashcode:String) ={
    val geohashMatch = new ArrayList[(String,Int)]
    var rs: ResultScanner = null
      try {
         rs =  hTable.getScanner(new Scan)
        val InputArray = new Array[Char](InputGeohashcode.length)
        val it = rs.iterator()
        var a,b,c = 0
        for(i <- InputGeohashcode){
          InputArray(b) = i
          b = b + 1
        }
        while(it.hasNext){
          val r = it.next()
          val rowkey =  Bytes.toString(r.getRow())
          val geohashCode = rowkey.split("-")(2)
          breakable{
            for (c <-Range(0,geohashCode.size)) {//0-11
              if(geohashCode(c)== InputArray(c)){
                a = a + 1 //统计前缀相同的位数
                if(a ==geohashCode.size){ //全部匹配
                  //println(rowkey,a)
                  geohashMatch.add((rowkey,a))
                  a = 0
                  break
                }
              }else{
               // println(rowkey,a)
                geohashMatch.add((rowkey,a))
                a = 0
                break
              }
            }}
        }}catch {case e: IOException => e.printStackTrace()}
      finally {
        if(rs != null) {
          rs.close()
        }
      }
    geohashMatch
  }
  //找到相似度最高的
  def findData(list:ArrayList[(String,Int)]) ={
    var max = list.get(0)._2
    var data = list.get(0)
    for( i <- Range(0,list.size())){
      if(max < list.get(i)._2){
        max = list.get(i)._2
        data = list.get(i)
      }
    }
    println(data)
    data
  }
  def delect_data(tileList:ArrayList[(Int, ArrayList[Array[Float]], (Double, Double), Double, (Int, Int), (Int, Int))]) ={
    val list = new ArrayList[(Int, (Double, Double), Double, (Int, Int), (Int, Int))]
    for (i <- Range(0, tileList.size)){
      val line = tileList.get(i)
      list.add((line._1,line._3,line._4,line._5,line._6))
    }
    list
  }
  def getAllNumber(left_down:(String,String),right_up:(String,String),timeStamp: Long, provinceNo: Long, cityNo: Long) ={
    println(new GeoHashTransform().encode(left_down._2.toDouble,left_down._1.toDouble))
    println(new GeoHashTransform().encode(right_up._2.toDouble,right_up._1.toDouble))
    val left_down_number = Bytes.toInt(searchHbase(findData(similarityMatch(new GeoHashTransform().encode(left_down._2.toDouble,left_down._1.toDouble))))) //左下hilbert值
    val right_up_number = Bytes.toInt(searchHbase(findData(similarityMatch(new GeoHashTransform().encode(right_up._2.toDouble,right_up._1.toDouble))))) //右上hilbert值
    println(left_down_number,right_up_number)
    val rangeList = new ArrayList[Int]
    //判断该经纬度范围是否跨瓦片
    if(left_down_number == right_up_number){//同一瓦片范围内
      println(left_down_number)
      rangeList.add(left_down_number)
    }else{
      val hilbert = new HilbertTransform
      val hilbertN = getHilbertN(timeStamp, provinceNo, cityNo)
      val left_down = hilbert.hilbertCodeToXY(hilbertN,left_down_number)//左下坐标
      val right_up = hilbert.hilbertCodeToXY(hilbertN,right_up_number)//右上坐标
      println(left_down,right_up)
      for(j <- Range(left_down._1,right_up._1 + 1)){
        for(i <- Range(right_up._2,left_down._2 + 1)){
          //println(i,j)
          println(hilbert.xyToHilbertCode(hilbertN,i,j))
          rangeList.add(hilbert.xyToHilbertCode(hilbertN,i,j))
        }
      }
    }
    rangeList
  }
  def getHilbertN(timeStamp: Long, provinceNo: Long, cityNo: Long) ={
    val hilbert = new HilbertTransform
    val ghbase = new GridDataToHBase
    val rowkey = Bytes.toBytes(ghbase.generateRowKey(timeStamp,provinceNo,cityNo,0.toLong))
    val rowNum =HBaseClient.selectOneColumnByRowKey(ghbase.tableName,rowkey,Bytes.toBytes("properties"),Bytes.toBytes("x"))
    val colNum =HBaseClient.selectOneColumnByRowKey(ghbase.tableName,rowkey,Bytes.toBytes("properties"),Bytes.toBytes("y"))
    println(rowNum,colNum)
    val hilbertN = hilbert.rowNumAndColNumGetN(Bytes.toInt(rowNum),Bytes.toInt(colNum))
    hilbertN
  }
}
