import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Auther: liyongchang
  * @Date: 2018/10/31 15:25
  * @Description:
  */
object Test {

  val conf = new SparkConf().setMaster("local").setAppName("test")
  val sc = new SparkContext(conf)

  def main(args: Array[String]):Unit = {
    //    val fileName = "2015-01.csv"
    //    val inputPath = "hdfs://masters/solap/airData/" + fileName
    val inputPath = "/Users/lyc/IdeaProjects/sparkDataProcess/data/2015-01.csv"
    val input = sc.textFile(inputPath,5)
    //val input = sc.parallelize(List(1,2,3,4,5,6))
    //缓存，一个rdd进行多次action操作时可使用
    input.persist()
    println(input.count())
    //take获取rdd的前n行数据
    input.take(100).foreach(println)
    //input.collect().foreach(println)
    //input.saveAsTextFile("resultData")
    println(input.partitioner)
  }

}
