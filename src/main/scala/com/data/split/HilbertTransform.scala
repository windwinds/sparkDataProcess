package com.data.split

/**
  * @Auther: liyongchang
  * @Date: 2018/12/3 10:41
  * @Description: Hilbert曲线编码
  */
class HilbertTransform {

  /**
    * x.y坐标转为Hilbert编码
    * @param n: 待编码单元格行列数，必须为2的n次方的某个数
    * @param x
    * @param y
    * @return
    */
  def xyToHilbertCode(n: Int, x: Int, y: Int):Int={

    var vx = x
    var vy = y
    var rx, ry, s, d = 0
    s = n/2
    while (s > 0){
      if((vx & s) > 0){
        rx = 1
      }else{
        rx = 0
      }
      if((vy & s) > 0){
        ry = 1
      }else{
        ry = 0
      }
      d += s*s*((3*rx)^ry)
      val xy = rot(s, vx, vy, rx, ry)
      vx = xy._1
      vy = xy._2
      s/=2
    }
    return d
  }

  /**
    * hilbert编码转为x，y坐标
    * @param n
    * @param hilbertCode
    * @return
    */
  def hilbertCodeToXY(n: Int, hilbertCode: Int):(Int, Int)={

    var x, y = 0
    var rx, ry, s, t = hilbertCode
    s = 1
    while ( s < n){
      rx = 1 & (t/2)
      ry = 1 & (t^rx)
      val xy = rot(s, x, y, rx, ry)
      x = xy._1
      y = xy._2
      x += s * rx
      y += s * ry
      t /= 4
      s = s * 2
    }
    (x, y)
  }


  def rot(n: Int, x: Int, y: Int, rx: Int, ry: Int):(Int, Int)={
    var a = x
    var b = y
    if ( ry == 0){
      if (rx == 1){
        a = n - 1 - a
        b = n - 1 - b
      }
      val t = a
      a = b
      b = t
    }
    (a, b)
  }

  /**
    * 根据矩阵行列数求N得值，返回2的N次方大于等于max(rowNum, colNum)的最小值
    * @param rowNum
    * @param colNum
    */
  def rowNumAndColNumGetN(rowNum: Int, colNum: Int)={
    val max = Math.max(rowNum, colNum)
    var n = 1
    while (Math.pow(2, n) < max){
      n+=1
    }
    Math.pow(2, n).toInt
  }




}
