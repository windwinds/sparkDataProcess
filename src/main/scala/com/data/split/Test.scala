package com.data.split

/**
  * @Auther: liyongchang
  * @Date: 2018/12/3 11:32
  * @Description:
  */
object Test {

  def main(args: Array[String]):Unit={

    val hilbert = new HilbertTransform()
    val n = 8
    for (i <- Range(0, n)){
      for (j <- Range(0, n)){
        val d = hilbert.xyToHilbertCode(n, j, n-i-1)
        print(d + " ")
      }
      println()
    }
    for (i <- Range(0, 64)){
      val xy = hilbert.hilbertCodeToXY(n, i)
      println(xy._1, xy._2)
    }


  }

}
