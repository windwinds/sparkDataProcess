package com.spark.utils

import java.io.File

object FileUtil {

  /**
    * 删除本地文件夹及其子文件
    * @param fileName
    */
  def deleteLocalFile(fileName: String):Unit={
    val dir = new File(fileName)
    val files = dir.listFiles()
    if (files != null){
      files.foreach(file => {
        if (file.isDirectory){
          deleteLocalFile(file.getPath)
        }else{
          file.delete()
        }
      })
      dir.delete()
      println("已删除本地文件夹： " + fileName)
    }else{
      println("本地文件夹不存在： " + fileName)
    }
  }







}
