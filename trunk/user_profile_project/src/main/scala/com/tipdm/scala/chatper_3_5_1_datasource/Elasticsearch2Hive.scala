package com.tipdm.scala.chatper_3_5_1_datasource

import com.tipdm.scala.chapter_3_8_6_svm.SVM.printUsage
import com.tipdm.scala.util.SparkUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql._

/**
  * //@Author:qwm
  * //@Date: 2018/8/16 13:41
  *
  * hiveTable:Hive表
  * selectedCols:ES资源同步的列字段名
  * timeColName:ES 资源时间列名称
  * timeColPattern:ES资源时间列格式,如 yyyyMMdd HH:mm:ss
  * timeRangeValue (required)    ES资源同步时间段值
  * timeRangeType (required)    ES资源同步时间段类型, Y|M|D
  * esTable:ES资源名，index/type
  * startTime:同步设置的这个时间之前的数据
  *
  */

object Elasticsearch2Hive {
  val defaultQuery: String = "?q=*:*"

  def main(args: Array[String]): Unit = {
    if (args.length != 9) {
      printUsage()
      System.exit(1)
    }
    val hiveTable = args(0)
    val selectedCols = args(1)
    val timeColName = args(2)
    val timeColPattern = args(3)
    val timeRangeValue = args(4).toInt
    val timeRangeType = args(5)
    val esTable = args(6)
    val startTime = args(7)
    val db = args(8)
    val options =
      Map(
        ("es.nodes", "192.168.2.162"),
        ("es.port", "9200"),
        ("es.read.metadata", "false"),
        ("es.mapping.date.rich", "false")
      )
    val conf = new SparkConf().setAppName("Elasticsearch2Hive")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    sqlContext.sql("set spark.sql.caseSensitive=true")
    val sparkTable: String = "tmp" + System.currentTimeMillis()
    val esDf = sqlContext.esDF(esTable, defaultQuery, options)
    esDf.registerTempTable(sparkTable)
    val sql = "CREATE TABLE " + db + "." + hiveTable + " as  select " + selectedCols + " from " + sparkTable +
      " where " + timeColName + " > '" + SparkUtils.getBeforeTimeStr(timeColPattern, timeRangeValue, timeRangeType, startTime) + "'"
    println("Hive中是否存在" + ":" + SparkUtils.exists(sqlContext, db, hiveTable))
    if (SparkUtils.exists(sqlContext, db, hiveTable)) {
      SparkUtils.dropTable(sqlContext, db, hiveTable)
      println("Hive中是否存在" + db + "." + hiveTable + ":" + SparkUtils.exists(sqlContext, db, hiveTable))
      sqlContext.sql(sql)
    } else {
      sqlContext.sql(sql)
    }
    sc.stop()
  }
  /**
    * 使用说明
    */
  def printUsage(): Unit = {
    val buff = new StringBuilder
    buff.append("Usage : com.tipdm.scala.chatper_3_5_1_datasource.Elasticsearch2Hive").append(" ")
      .append("<hiveTable>").append(" ")
      .append("<selectedCols>").append(" ")
      .append("<timeColName>").append(" ")
      .append("<timeColPattern>").append(" ")
      .append("<timeRangeValue>").append(" ")
      .append("<timeRangeType>").append(" ")
      .append("<esTable>").append(" ")
      .append("<startTime>").append(" ")
      .append("<db>").append(" ")
    println(buff.toString())
  }
}
