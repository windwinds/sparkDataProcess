package com.spark.solap

import java.util

/**
  * @Auther: liyongchang
  * @Date: 2018/11/2 15:43
  * @Description: solap操作算子实现
  */
class SolapOperation {

  val timeDimensions = Array("hour", "day", "month", "year")
  val spaceDimensions = Array("city", "province", "country")
  val dimensionsMap = new util.HashMap[String, Array[String]]
  dimensionsMap.put("time", timeDimensions)
  dimensionsMap.put("space", spaceDimensions)

  /**
    * 上卷操作，包括时间维层次、空间维层次上的操作
    * @param dimensionType: time、space
    * @param nowDimension
    * @param rowKey
    */
  def rollUp(dimensionType: String, nowDimension: String, rowKey: Long)={

    val dimensionsArray = dimensionsMap.get(dimensionType)
    if (dimensionsArray == null){
      println("无效的维层次")
      sys.exit()
    }

    //时间维度上卷
    if ("time".equals(dimensionType)){



    }
    //空间维度上卷
    else if("space".equals(dimensionType)){

    }else{

    }



  }



  def drillDown(dimensionType: String, nowDimension: String, rowKey: Long): Unit ={

  }


}
