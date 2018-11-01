package com.spark.etl

import org.apache.spark.rdd.RDD

class AirDataETL extends Serializable {

  /**
    * 判断数组中包含空值的数据
    * @param array
    * @return
    */
  def isContainNull(array: Array[String]):Boolean={

    for (value <- array){
      if (value == null || value == "")
        return true
    }
    return false
  }

  def cleanWrongData(inputRDD: RDD[String]):RDD[Array[String]]={

    inputRDD.map(x => x.split(",")).filter(isContainNull)


  }


}
