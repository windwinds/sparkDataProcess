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

  /**
    * 获取某目录下的所有文件
    */
  def getFiles1(dir: File): Array[File] = {
    dir.listFiles.filter(_.isFile) ++
      dir.listFiles.filter(_.isDirectory).flatMap(getFiles1)
  }

  //获取任意多个目录下所有文件
  def getFiles(inputs: File*): Seq[File] = {
    inputs.filter(_.isFile) ++
      inputs.filter(_.isDirectory).flatMap(dir => getFiles(dir.listFiles: _*))
  }


}
