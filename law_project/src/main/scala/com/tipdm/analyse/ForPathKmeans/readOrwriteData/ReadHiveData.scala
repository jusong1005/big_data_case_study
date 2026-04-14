package com.tipdm.analyse.ForPathKmeans.readOrwriteData

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * suling
  * 读取Hive表中的数据
  */
object ReadHiveData {

  def readData(inputTable: String, hiveContext: HiveContext): DataFrame = {
    hiveContext.sql("select * from " + inputTable)
  }

  def readData(inputTable: String): DataFrame = {
    val hiveContext = getSparkSession()
    hiveContext.sql("select * from " + inputTable)
  }

  def main(args: Array[String]): Unit = {
    val hiveContext = getSparkSession()
    val data = readData("law_init1.lawtime_gt_one_distinct", hiveContext)
    data.show(20)
    data.select(fullurl).distinct()
    data.select(userid).distinct()
  }
}
