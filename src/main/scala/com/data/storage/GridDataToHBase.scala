package com.data.storage

import com.data.database.HBaseClient

/**
  * @Auther: liyongchang
  * @Date: 2018/12/13 11:04
  * @Description:
  */
class GridDataToHBase {

  val tableName = "GridAirDataTbl"

  val hTable = HBaseClient.getHTableByName(tableName)





}
