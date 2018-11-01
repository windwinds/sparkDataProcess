package com.spark.config

import org.apache.spark.{SparkConf, SparkContext}

class SparkConfig {

  def getSparkContext(environment: String):SparkContext = {

    if (environment.equals("local")){
      val conf = new SparkConf().setMaster("local").setAppName("test")
      new SparkContext(conf)
    }else{
      val conf = new SparkConf().setAppName(environment)
      new SparkContext(conf)
    }

  }




}
