package com.data.split

import java.io._
import java.util.ArrayList

import breeze.linalg.DenseMatrix
import com.data.split.Test.arrayToList
import com.data.storage.GridDataToHBase
import org.apache.hadoop.hbase.util.Bytes
import src.main.scala.com.data.storage.TilesRangeHbase

import scala.io.Source
import scala.collection.mutable.ArrayBuffer

/*
*@Description:瓦片拼接
*@Return:${returns}
*@Author:LiumingYan
*@Date 11:332019/3/22
*/ class TileSplicing {
  var ncols,nrows = 0
  var cellsize,xllcorner,yllcorner,NODATA_value = 0.toDouble
  def tileSplicing(all_Number:ArrayList[Int],timestamp:Long,provinceNo:Long,cityNo:Long,pollutionName:String,all_Map:Map[Int,((Double, Double), Double, (Int, Int), (Int, Int))],nodata_Value:Double) ={
    NODATA_value = nodata_Value
    val h = new HilbertTransform
    val t = new TilesRangeHbase
    val gridDataToHBase = new GridDataToHBase
    val pollutionList = Array("aqi", "pm25", "pm10", "co", "no2", "ozone1hour", "ozone8hour", "so2")
    val randomFile = new RandomAccessFile(new File("data/temp"),"rw") //存矩阵的临时文件

    var site =Map[(Int,Int),(Int,Int)]()//存坐标以及对应长宽
    val xy_range = all_Map(0)._4
    cellsize = all_Map(0)._2
    var number= 0
    var n = xy_range._1 - 1
    val hilbertN = t.getHilbertN(timestamp, provinceNo, cityNo) //获取hilbertN
    var last_mat = DenseMatrix.zeros[Float](16,1)
    //填Map
    for(i <- Range(0,all_Number.size())) {
      val hw = all_Map(all_Number.get(i))._3 //查all_Map得到长宽
      val xy = h.hilbertCodeToXY(hilbertN, all_Number.get(i))
      site += (xy -> hw)
    }
    val min_col = MinCol(site)
    val min_row = MinRow(site)
    val max_row = MaxRow(site)
      while(n >= 0){
      for(m <-Range(0,xy_range._2)){
        println(m,n)//列 行 x,y
        //检查key是否存在
        if (site.contains((m,n))) {
          //从gridAirData表中查找该瓦片
          val data = gridDataToHBase.searchData(timestamp, 1, 1, h.xyToHilbertCode(hilbertN,m,n), arrayToList(pollutionList), Bytes.toBytes("data"))
          val data_mat = new DenseMatrix(site((m,n))._2,site((m,n))._1,data(pollutionName)).t//创建矩阵，交换行列，转置
          if(number == 0){
            last_mat = data_mat
          }else{
            last_mat = DenseMatrix.horzcat(last_mat,data_mat)
          }
          number = number + 1
          println(site((m,n)))
        }
      }
        if(n <= max_row - 1){
          last_mat = delectZeros(last_mat)
          ncols = last_mat.cols
          nrows = nrows + last_mat.rows
        }else{
          nrows = nrows + last_mat.rows
        }
        for (i <- Range(0, last_mat.rows )) {
          for (j <- Range(0, last_mat.cols )) {
            randomFile.writeBytes(last_mat(i, j).toString) //写入数据
            randomFile.writeBytes("\t")
            }
          randomFile.writeBytes("\n") //写入数据
        }
        n = n - 1
        if(n >= min_row) {
          last_mat = DenseMatrix.zeros[Float](site(min_col, n)._1, site(min_col, n)._2)
        }
      }
        val out = new PrintWriter("data/"+(provinceNo).toString+(cityNo).toString+pollutionName)//真的文件
        xllcorner = all_Map.get(h.xyToHilbertCode(hilbertN,min_col,max_row)).get._1._1
        yllcorner = all_Map.get(h.xyToHilbertCode(hilbertN,min_col,max_row)).get._1._2
        out.println("ncols"+ "\t" + ncols)
        out.println("nrows"+ "\t" + nrows)
        out.println("xllcorner"+ "\t" + xllcorner)
        out.println("yllcorner"+ "\t" + yllcorner)
        out.println("cellsize"+ "\t" + cellsize)
        out.println("NODATA_value"+ "\t" + NODATA_value)
        for( i<- Source.fromFile("data/temp").getLines().toArray){
          out.println(i)
        }
        out.close()
  }
  def MinCol(site:Map[(Int,Int),(Int,Int)]) ={
    var col_min = 16
    val allKey = site.keys
    for(x <- allKey){
      if(x._1 < col_min)
        col_min = x._1
    }
    col_min
  }
  def MinRow(site:Map[(Int,Int),(Int,Int)]) ={
    var row_min = 16
    val allKey = site.keys
    for(y <- allKey){
      if(y._2 < row_min)
        row_min = y._2
    }
    row_min
  }
  def MaxRow(site:Map[(Int,Int),(Int,Int)]) ={
    var row_max = 0
    val allKey = site.keys
    for(y <- allKey){
      if(y._2 > row_max)
        row_max = y._2
    }
    row_max
  }
  def delectZeros( last_mat:DenseMatrix[Float])={
    val data = new ArrayBuffer[Float]()
    var x = last_mat.rows
    var y = last_mat.cols
    var last_mat_new = new DenseMatrix(1,1,Array(0.0.toFloat))
    for(i <- Range(0,last_mat.rows)){
      for(j <- Range(0,last_mat.cols)){
        if(last_mat(i,j)!= 0){
          data += last_mat(i,j)
        }
        else{
          y = y - 1
        }
      }
      if(y == 0)
        x = x -1
      if(i != last_mat.rows-1)
        y = last_mat.cols
    }
    if(data.length != 0){
      last_mat_new =  new DenseMatrix[Float](y, x, data.toArray).t
    }
    println(last_mat_new)
    last_mat_new
  }

}
