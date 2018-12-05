package com.data.split

import java.util.ArrayList

import scala.io.Source

/**
  * @Auther: liyongchang
  * @Date: 2018/12/3 10:42
  * @Description: 将插值后的数据进行瓦片划分
  */
class TileSplit(fileName: String) {

  var ncols = 0
  var nrows = 0
  //左下角经纬度
  var xllcorner = 0.0
  var yllcorner = 0.0
  //像元大小
  var cellsize = 0.0
  //无效值
  var NODATA_value = 0.0
  val data = new ArrayList[ArrayList[Float]]()


  /**
    * 解析ASC文件，并初始化成员变量
    */
  def readAscFileAndInitData()={

    val file = Source.fromFile(fileName)

    val lines = file.getLines()

    ncols = lines.next().split("\\s+")(1).toInt
    nrows = lines.next().split("\\s+")(1).toInt
    xllcorner = lines.next().split("\\s+")(1).toDouble
    yllcorner = lines.next().split("\\s+")(1).toDouble
    cellsize = lines.next().split("\\s+")(1).toDouble
    NODATA_value = lines.next().split("\\s+")(1).toDouble

    while(lines.hasNext){

      val rowData = new ArrayList[Float]
      val rowDataStrArray = lines.next().split("\\s+")
      for (i <- Range(0, rowDataStrArray.length)){
        rowData.add( rowDataStrArray(i).toFloat )
      }
      data.add(rowData)
    }

  }

  /**
    * 将数据划分为瓦片块
    * tileLength: 瓦片长宽
    * 返回多元组List((x,y), tileData, (lng, lat), cellSize, (tileHeight, tileWidth), (tileRowNum, tileColNum))，（
    * x, y）为瓦片单元的横纵坐标，起始点为整个二维数组的左下角，该处的瓦片单元坐标为(0, 0)
    * (lng, lat)为瓦片右上角像素经纬度，cellSize为像素大小，(tileLength, tileWidth)为瓦片实际长宽，边界的瓦片不一样
    * 瓦片内部像素从左上角开始写
    */
  def splitTile(tileLength: Int)={

    if (data.size() == 0 || data.get(0).size() == 0){
      println("瓦片切分失败：没有数据")
      sys.exit()
    }

    //瓦片行列数，x:0->tileColNum-1, y: 0->tileRowNum-1
    val tileRowNum = data.size()/tileLength + 1
    val tileColNum = data.get(0).size()/tileLength + 1

    val tileRowColNum = (tileRowNum, tileColNum)
    val resultList = new ArrayList[((Int, Int), Array[Float], (Double, Double), Double, (Int, Int), (Int, Int))]()

    for (i <- Range(0, tileRowNum)){
      var startRowNum = nrows - (i + 1)*tileLength
      var less = 0
      if (startRowNum < 0){
        less = nrows - (i + 1)*tileLength
        startRowNum = 0
      }
      //包含每层瓦片的List，一共0-tileRowNum层
      val tileRowList = new ArrayList[ArrayList[Float]]()
      for (j <- Range(startRowNum, startRowNum + tileLength + less)){
        tileRowList.add(data.get(j))
      }
      //瓦片左上角纬度
      var lat = yllcorner + (nrows - startRowNum-1) * cellsize
      //瓦片实际高度，最上面的瓦片可能不同
      val tileHeight = tileRowList.size()
      for (col <- Range(0, tileColNum)){

        //瓦片实际长度，最右边的瓦片肯能不同
        var tileWidth = 0
        if ( ((col+1)*tileLength) <= ncols ){
          tileWidth = tileLength
        }else{
          tileWidth = ncols - col * tileLength
        }
        val xy = (col, i)
        val tileData = new Array[Float](tileHeight*tileWidth)
        //瓦片左上角经度
        var lng = xllcorner + col * tileLength * cellsize
        val lnglat = (lng, lat)
        val hightWidth = (tileHeight, tileWidth)
        var index = 0
        for (n <- Range(0, tileRowList.size())){
          for (m <- Range(col*tileLength, col*tileLength + tileWidth)){
            tileData(index) = tileRowList.get(n).get(m)
            index+=1
          }
        }
        resultList.add((xy, tileData, lnglat, cellsize, hightWidth, tileRowColNum))


      }

    }
    resultList

  }


  def tileNumToHilbertCode(tileList: ArrayList[((Int, Int), Array[Float], (Double, Double), Double, (Int, Int), (Int, Int))])={
    if (tileList == null || tileList.size()==0){
      println("传入的瓦片数据为空！")
      sys.exit()
    }
    val hilbert = new HilbertTransform()
    val rowColNum = tileList.get(0)._6

    val hilbertN = hilbert.rowNumAndColNumGetN(rowColNum._1, rowColNum._2)

    val resultList = new ArrayList[(Int, Array[Float], (Double, Double), Double, (Int, Int), (Int, Int))]()

    for (i <- Range(0, tileList.size())){

      val x = tileList.get(i)._1._1
      val y = tileList.get(i)._1._2
      val hilbertCode = hilbert.xyToHilbertCode(hilbertN, x, y)
      val tile = tileList.get(i)
      resultList.add(hilbertCode, tile._2, tile._3, tile._4, tile._5, tile._6)

    }
    resultList

  }




}
