package com.spark.solap

import java.util

import com.data.split.HilbertTransform
import com.spark.utils.DataTransformUtil
import org.apache.spark.rdd.RDD

/**
  * @Auther: liyongchang
  * @Date: 2019/3/16 10:23
  * @Description:
  */
class SpatioTemporalQuery extends Serializable {


  /**
    * 空气质量逐日、月、季度、年变化规律
    * eg:查询湖北省武汉市2016年5月1日PM2.5每小时浓度变化规律。
    * 时间查询中，地理位置都相同，不需要考虑地点，只需要根据时间维层次的不同进行聚集
    */
  def regularPatternInDay(tileRdd: RDD[(Long, Long, (Int, Int, Double, Double, Double, Int, Int, Double), Array[Float])], timeDimensionHierarchy: String)={

    tileRdd.map(x => {
      var date = "0000"
      if (timeDimensionHierarchy.equals("hours")){
        date = DataTransformUtil.timestampToDate(x._2, "hours")
      }
      if (timeDimensionHierarchy.equals("day")){
        date = DataTransformUtil.timestampToDate(x._2, "day")
      }
      if (timeDimensionHierarchy.equals("month")){
        date = DataTransformUtil.timestampToDate(x._2, "month")
      }
      if (timeDimensionHierarchy.equals("season")){
        date = DataTransformUtil.timestampToDate(x._2, "season")
      }
      (date, x._4)
    }).groupByKey()
      .map(kv => {

        val key = kv._1
        val valueIterable = kv._2
        val valueIterator = valueIterable.iterator
        //Iterator只能遍历一遍，调用了size后，就不能遍历了
        //val tileNum = valueIterable.size
        var sum = 0.0
        var validTileNum = 0
        var i = 0
        while (valueIterator.hasNext){
          val floatArray = valueIterator.next()
          var floatSum = 0.0
          //有效值个数
          var num = 0
          for (f <- floatArray){
            if (f != -999.0){
              floatSum = floatSum + f
              num = num + 1
            }

          }
          if (num != 0){
            sum = sum + floatSum/num
            validTileNum = validTileNum + 1
          }
          i = i + 1
        }
        val result = sum/validTileNum
        (key, result)
      }).sortByKey()

  }

  /**
    * 空间查询
    *查询湖北省武汉市在2016-03-27的pm25热力图
    */
  def spatialQuery(tileRdd: RDD[(Long, Long, (Int, Int, Double, Double, Double, Int, Int, Double),
    Array[Float])], spatialDimensionHierarchy: String)={

    tileRdd.map(x => {
      //provinceCode+cityCode+tileCode
      val key = x._1
      val value = (x._4, x._3)
      (key, value)
    })
      .groupByKey()
      .map(kv => {

        val key = kv._1
        //相同瓦片编号的不同时间的瓦片集合
        val valueIterable = kv._2
        val valueIterator = valueIterable.iterator
        val size = valueIterable.size
        val tileArray = new Array[Array[Float]](size)
        var i = 0
        val properties = new util.ArrayList[(Int, Int, Double, Double, Double, Int, Int, Double)]()
        while (valueIterator.hasNext){
          val nextValue = valueIterator.next()
          val floatArray = nextValue._1
          if (properties.size() == 0){
            properties.add(nextValue._2)
          }
          tileArray(i) = floatArray
          i = i + 1
        }
        val tileSize = tileArray(0).length
        val resultTile = new Array[Float](tileSize)
        for (k <- Range(0, tileSize)){

          var sum = 0.0f
          var validCount = 0
          for (index <- Range(0, size)){
            //println("k: " + k + "index: " + index)
            val v = tileArray(index)(k)
            if (v != -999.0){
              sum = sum + v
              validCount = validCount + 1
            }
          }
          if (validCount != 0){
            resultTile(k) = sum/validCount
          }else{
            resultTile(k) = -999.0.toFloat
          }

        }
        (key, (resultTile, properties.get(0)))
      })

  }

  /**
    * 将瓦片合并为完整的栅格文件，根据瓦片索引和属性信息，确定瓦片中每个像素在整个栅格文件中的索引
    */
  def tileToAscRaster(tileRdd: RDD[(Long, (Array[Float], (Int, Int, Double, Double, Double, Int, Int, Double)))],
                      ascInfoMap: util.HashMap[Long, (Int, Int, Double, Double, Double, Double, Int)])={

    tileRdd.flatMap(x => {

      val provinceCityNo = x._1/100000

      val hilbertCode = x._1%100000
      //x、y方向瓦片个数
      val xTileCount = x._2._2._1
      val yTileCount = x._2._2._2
      val hilbertTransform = new HilbertTransform
      val n = hilbertTransform.rowNumAndColNumGetN(yTileCount, xTileCount)
      val xyIndex = hilbertTransform.hilbertCodeToXY(n, hilbertCode.toInt)

      //当前瓦片长宽
      val tileHight = x._2._2._6
      val tileWidth = x._2._2._7
      val floatArray = x._2._1

      val ascProperties = ascInfoMap.get(provinceCityNo)
      val ncol = ascProperties._1
      val nrow = ascProperties._2

      val hwArray = getIndexInTile(floatArray, tileHight, tileWidth)
      val indexArray = getIndexInAsc(hwArray, ncol, xyIndex._1, xyIndex._2, ascProperties._7)
      //调整顺序
      val reSetIndexArray = reSetIndexInAsc(indexArray, ncol, nrow)

      //根据index计算在整个asc中的行号，形成（行号，（index， floatValue））的键值对
      val propertiesLen = 7
      val result = new Array[(Int, (Int, Float, Double))](floatArray.length + propertiesLen)
      //前面存properties, 为了使最终的行数与原始文件行数相等
      result(0) = (0, (0, 0, ncol))
      result(1) = (1, (0, 0, nrow))
      result(2) = (2, (0, 0, ascProperties._3))
      result(3) = (3, (0, 0, ascProperties._4))
      result(4) = (4, (0, 0, ascProperties._5))
      result(5) = (5, (0, 0, ascProperties._5))
      result(6) = (6, (0, 0, ascProperties._6))

      for (i <- Range(0, floatArray.length)){

        val index = reSetIndexArray(i)
        val indexRow = index/ncol
        result(i+propertiesLen) = (indexRow+propertiesLen, (index, floatArray(i), 0))
      }
      result
    })
      .groupByKey()
      .map(kv => {
        val valueIterable = kv._2
        val valueIterator = valueIterable.iterator
        val propertiesLen = 7
        val property = Array("NCOLS", "NROWS", "XLLCENTER", "YLLCENTER", "DX", "DY", "NODATA_VALUE")
        val row = kv._1
        if ( row >= 0 && row < propertiesLen){
          val firstValue = valueIterator.next()
          var valueStr = ""
          if (row == 0 || row == 1){
            valueStr = property(row) + " " +firstValue._3.toInt
          }else{
            valueStr = property(row) + " " + firstValue._3
          }
          (row, valueStr)
        }else{

          val size = valueIterable.size
          val indexArray = new Array[Int](size)
          val floatArray = new Array[Float](size)
          var i = 0
          while (valueIterator.hasNext){
            val value = valueIterator.next()
            indexArray(i) = value._1
            floatArray(i) = value._2
            i = i + 1
          }
          //根据indexArray,将floatArray排序,数组中绝大部分有序，采用插入排序
          insertSort(indexArray, floatArray)
          var str = ""
          for (f <- floatArray){
            str = str + f + " "
          }
          (row, str)
        }
      })
      .sortByKey()

  }

  /**
    * 得到瓦片中每个像元在瓦片中的高宽
    * array(h, w)
    */
  def getIndexInTile(floatArray: Array[Float], tileHight: Int, tileWidth: Int)={

    val len = floatArray.length
    val hwArray = new Array[(Int, Int)](len)

    for (i <- Range(0, len)){
      val w = i % tileWidth   //从1开始会出错，不能再加1
      val h = tileHight - i/tileWidth

      hwArray(i) = (h ,w)

    }
    hwArray

  }

  /**
    * 计算瓦片中的每个点在整个asc中的索引号i（从左下角开始，从左到右，从下到上，左下角i=0）
    */
  def getIndexInAsc(hwArray: Array[(Int, Int)], ncol: Int, x: Int, y: Int, tileLength: Int)={

    val len = hwArray.length
    val indexResult = new Array[Int](len)
    for (i <- Range(0, len)){

      val hw = hwArray(i)

      val index = (x*tileLength + hw._2) + (hw._1 - 1 + y*tileLength)*ncol

      indexResult(i) = index

    }
    indexResult

  }

  /**
    * 第一次计算的Index是从下到上，从左到右的，需要重置为：从上到下，从左到右
    */
  def reSetIndexInAsc(indexArray: Array[Int], ncol: Int, nrow: Int)={
    val len = indexArray.length
    val result = new Array[Int](len)
    for (i <- Range(0, len)){
      val index = indexArray(i)

      val reSetIndex = (nrow - (index/ncol) - 1)*ncol + index%ncol

      result(i) = reSetIndex
    }
    result
  }

  /**
    * 根据arrA的值对arrB排序
    * @param arrA
    * @param arrB
    */
  def insertSort(arrA: Array[Int], arrB: Array[Float])={

    for (i <- Range(1, arrA.length)){

      for (j <- Range(0, i)){

        if (arrA(j) > arrA(i)){
          val temp = arrA(i)
          arrA(i) = arrA(j)
          arrA(j) = temp

          val tt = arrB(i)
          arrB(i) = arrB(j)
          arrB(j) = tt

        }

      }

    }

  }


}
