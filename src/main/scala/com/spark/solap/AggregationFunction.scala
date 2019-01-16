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
  def tileAvg(tileRdd: RDD[(Long, Int, Int, Double, Array[Float])])={

    tileRdd.map(x => {
      val noData = x._4
      val floatArray = x._5
      var sum = 0.0
      var count = 0
      for (floatValue <- floatArray){
        if (floatValue != noData){
          sum += floatValue
          count += 1
        }
      }
      if (sum == 0){
        ("noData", (1, 0))
      }else{
        ("avg", (1, sum/(count.toFloat)))
      }
    }).reduceByKey((x,y) => {
      (x._1 + y._1, x._2 + y._2)
    }).mapValues(x => x._2/(x._1.toFloat)).collect()

  }


  def tileMax()={

  }


  def tileMin()={

  }


}
