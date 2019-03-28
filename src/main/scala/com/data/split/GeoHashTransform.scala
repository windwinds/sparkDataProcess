package com.data.split

import java.util
import java.lang.{Long => JLong}
/*
*@Description:实现经纬度与GeoHash的编码与反码
*@Author:LiumingYan
*/
class GeoHashTransform {
  def decode(geoHash: String):Array[Double] = {
    val buffer = new StringBuilder
    for (c <- geoHash.toCharArray) {
      val i = GeoHashTransform.lookup.get(c) + 32
      buffer.append(Integer.toString(i, 2).substring(1))
    }
    val lonset = new util.BitSet
    val latset = new util.BitSet
    // even bits
    var j = 0
    for  (i <- Range(0,GeoHashTransform.numbits * 2,2)) {
      var isSet = false
      if (i < buffer.length)
        isSet = buffer.charAt(i) == '1'
      lonset.set(j,isSet)
      j = j + 1
    }
    // odd bits
    j = 0
    for(i <- Range(1,GeoHashTransform.numbits * 2,2)){
      var isSet = false
      if (i < buffer.length)
        isSet = buffer.charAt(i) == '1'
      latset.set(j,isSet)
      j = j + 1
    }
    val lon = deco(lonset, -180, 180)
    val lat = deco(latset, -90, 90)
    val a = Array[Double](lat, lon)
    a
  }

  private def deco(bs: util.BitSet, floor: Double, ceiling: Double) = {
    var mid = 0.0000
    var fl = floor
    var ce = ceiling
    for (i <- Range(0,bs.length,1)){
      mid = (fl + ce) / 2
      if (bs.get(i))
        fl = mid
      else
        ce = mid
    }
    mid
  }

  def encode(lat: Double, lon: Double): String = {
    val latbits = getBits(lat, -90, 90)
    val lonbits = getBits(lon, -180, 180)
    val buffer = new StringBuilder
    for(i <- Range(0,GeoHashTransform.numbits,1)){
      if(lonbits.get(i)){
        buffer.append('1')
      }else{
        buffer.append('0')
      }
      if(latbits.get(i)){
        buffer.append('1')
      }else{
        buffer.append('0')
      }
    }
    GeoHashTransform.base32(JLong.parseLong(buffer.toString,2))
  }

  private def getBits(d: Double, floor: Double, ceiling: Double) = {
    val buffer = new util.BitSet(GeoHashTransform.numbits)
    var ceil = ceiling
    var fl = floor
    for (i <- Range(0,GeoHashTransform.numbits,1)){
      val mid = (fl + ceil) / 2
      if (d >= mid) {
        buffer.set(i)
        fl = mid
      }
      else
        ceil = mid
    }
    buffer
  }
}
object GeoHashTransform {
  private val numbits = 6 * 5
  val digits = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')
  val lookup = new util.HashMap[Character, Integer]
  var i = 0
  for (c <- digits) {
    lookup.put(c, i)
    i = i + 1
  }

  private def base32(i: Long): String = {
    val buf = new Array[Char](65)
    var charPos = 64
    val negative: Boolean = (i < 0)
    var j = i
    if (!negative) {
      j = -1 * j
    }
    while (j <= -32) {
      buf(charPos) = digits((-(j % 32)).toInt)
      charPos = charPos - 1
      j = j / 32
    }
    buf(charPos) = digits(-(j).toInt)
    if (negative) {
      charPos = charPos - 1
      buf(charPos) = '-'
    }
    val s = new String(buf, charPos, 65 - charPos)
    s
  }

  def main(args: Array[String]): Unit = {
    val a = new GeoHashTransform()
    println(a.encode(41.041978, 115.41381))
    val array = new GeoHashTransform().decode("uzvpvzvzgruz")
    for (a <- array) {
      println(a)
    }
  }
}
