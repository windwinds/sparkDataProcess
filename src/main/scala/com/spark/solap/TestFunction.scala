package com.spark.solap

import com.spark.config.SparkConfig

/**
  * @Auther: liyongchang
  * @Date: 2019/1/16 17:04
  * @Description:
  */
object TestFunction {

  val SparkConfig = new SparkConfig()
  val sc = SparkConfig.getSparkContext("local")

  def main(args: Array[String]): Unit = {
    val inputRdd = sc.parallelize(List(("a",10),("b",4),("a",10),("b",20)))
    val rdd1 = inputRdd.mapValues(x => (x, 1))
    val rdd2 = rdd1.reduceByKey((x, y) =>
      (x._1 + y._1, x._2 + y._2)
    )
    val rdd3 = rdd2.mapValues(x => (x._1, x._2, x._1.toDouble / x._2.toDouble)).collect()
//    rdd3.collect()
    inputRdd.collect()
  }

}
