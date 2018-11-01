package com.spark.bootstrap

import com.spark.config.SparkConfig
import com.spark.etl.AirDataETL
import com.spark.utils.FileUtil

object StartApplication {

  def main(args: Array[String]):Unit={

    val SparkConfig = new SparkConfig()
    val sc = SparkConfig.getSparkContext("local")
    val inputPath = "/Users/lyc/IdeaProjects/sparkDataProcess/data/2015-01.csv"
    val outputPath = "resultData";
    val input = sc.textFile(inputPath)

    val airDataETL = new AirDataETL()
    val cleanData = airDataETL.cleanWrongData(input)

    println(cleanData.count())

    FileUtil.deleteLocalFile(outputPath)


  }

}
