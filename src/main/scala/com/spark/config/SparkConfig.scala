package com.spark.config

import org.apache.spark.{SparkConf, SparkContext}

class SparkConfig {

  def getSparkContext(environment: String):SparkContext = {

    if (environment.equals("local")){
      val conf = new SparkConf().setMaster("local").setAppName("test")
      setConf(conf)
      new SparkContext(conf)
    }else{
      val conf = new SparkConf().setAppName(environment).setMaster("spark://master:7077")
      setConf(conf)
      new SparkContext(conf)
    }

  }

  def setConf(sparkConf: SparkConf): Unit ={
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.registerKryoClasses(Array(classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable], classOf[com.spark.solap.SpatioTemporalQuery]))
  }




}
