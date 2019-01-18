package com.spark.solap

import org.apache.spark.rdd.RDD

/**
  * @Auther: liyongchang
  * @Date: 2019/1/3 15:46
  * @Description: 基于瓦片的Spark分布式聚集函数
  */
class AggregationFunction {


  /**
    * 基于瓦片的均值聚集函数
    */
  def tileAvg(tileRdd: RDD[(Long, Int, Int,Array[Float])])={

    tileRdd.map(x => {
      val floatArray = x._4
      var sum = 0.0
      var count = 0
      for (floatValue <- floatArray){
        if (floatValue != -9999.0){
          sum += floatValue
          count += 1
        }
      }
      if (sum == 0){
        ("noData", (1, 0.0))
      }else{
        ("avg", (1, sum/(count.toFloat)))
      }
    }).reduceByKey((x,y) => {
      (x._1+y._1, x._2+y._2)
    }).mapValues(x => {
      x._2/x._1.toFloat
    }).filter(x => {
      x._1.equals("avg")
    }).first()

  }


  def tileMax(tileRdd: RDD[(Long, Int, Int,Array[Float])])={

    tileRdd.map(x => {

      val floatArray = x._4
      var max = 0.0
      for (floatValue <- floatArray){
        if (floatValue != -9999.0){
          max = Math.max(max, floatValue)
        }
      }
      max
    }).reduce((x, y) => {
      Math.max(x, y)
    })

  }


  def tileMin(tileRdd: RDD[(Long, Int, Int,Array[Float])])={

    tileRdd.map(x => {

      val floatArray = x._4
      var min = 9999.0
      for (floatValue <- floatArray){
        if (floatValue != -9999.0){
          min = Math.min(min, floatValue)
        }
      }
      min
    }).reduce((x, y) => {
      Math.min(x, y)
    })

  }


}
