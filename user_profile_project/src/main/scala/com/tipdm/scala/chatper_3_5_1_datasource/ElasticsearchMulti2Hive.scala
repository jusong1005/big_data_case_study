package com.tipdm.scala.chatper_3_5_1_datasource

import com.tipdm.scala.chatper_3_5_1_datasource.Elasticsearch2Hive.printUsage
import com.tipdm.scala.util.SparkUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.hadoop.rest.RestClient
import org.elasticsearch.spark.cfg.SparkSettingsManager
import org.elasticsearch.spark.sql._

import scala.collection.JavaConverters._

/**
  * //@Author:qwm
  * //@Date: 2018/8/16 14:35
  *
  * hiveTable:Hive表
  * selectedCols:ES资源同步的列字段名
  * tsColName:ES 资源时间列名称
  * tsColPattern:ES资源时间列格式,如 yyyyMMdd HH:mm:ss
  * timeRangeValue (required)    ES资源同步时间段值
  * timeRangeType (required)    ES资源同步时间段类型, Y|M|D
  * esIndexType：ES资源名index前缀类型，yyyyMMdd|yyyyMM|yyyyww
  * esIndexPre:ES资源名index前缀，index prefix
  * startTime:同步startTime之前的数据
  */
object ElasticsearchMulti2Hive {
  val default_query: String = "?q=*:*"

  def main(args: Array[String]): Unit = {
    if (args.length != 11) {
      printUsage()
      System.exit(1)
    }
    val hiveTable = args(0)
    val selectedCols = args(1)
    val tsColName = args(2)
    val tsColPattern = args(3)
    val timeRangeValue = args(4).toInt
    val timeRangeType = args(5)
    val esIndexType = args(6)
    val esIndexPre = args(7)
    val esType = args(8)
    val startTime = args(9)
    val db = args(10)
    val options = Map(
      ("es.nodes", "192.168.2.162"),
      ("es.port", "9200"),
      ("es.read.metadata", "false"),
      ("es.mapping.date.rich", "false"),
      ("es.read.field.as.array.include", "vod_cat_tags")
    )
    val conf = new SparkConf().setAppName("media_index_3m")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    sqlContext.sql("set spark.sql.caseSensitive=true")
    val sparkTable: String = "tmp" + System.currentTimeMillis()
    val sql = "CREATE TABLE " + db + "." + hiveTable + " as  select " + selectedCols + " from " + sparkTable + " where " + tsColName + " > '" + SparkUtils.getBeforeTimeStr(tsColPattern, timeRangeValue, timeRangeType, startTime) + "'"
    val selectedColsArr = selectedCols.split(",")
    val firstCol = selectedColsArr(0).trim
    val tailCols = selectedColsArr.slice(1, selectedColsArr.length).map(_.trim)
    val allTables = SparkUtils.getBeforeTimeTableNames(tsColPattern, timeRangeValue, timeRangeType, esIndexPre, esIndexType, esType, startTime)
    println("allTables:" + allTables)
    // 先判断表是否存在
    val settings = new SparkSettingsManager().load(sqlContext.sparkContext.getConf).merge(options.asJava)
    val client = new RestClient(settings)
    val allExistTables = allTables.filter { x => val s = x.split("/"); client.typeExists(s(0), s(1)) }
    client.close()
    println("allExistTables : " + allExistTables)
    val esDf = allExistTables.map(x => sqlContext.esDF(x, default_query, options))
      .reduce((x1, x2) => x1.select(firstCol, tailCols: _*).unionAll(x2.select(firstCol, tailCols: _*)))
    esDf.registerTempTable(sparkTable)
    if (SparkUtils.exists(sqlContext, db, hiveTable)) {
      SparkUtils.dropTable(sqlContext, db, hiveTable)
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
    buff.append("Usage : com.tipdm.scala.chatper_3_5_1_datasource.ElasticsearchMulti2Hive").append(" ")
      .append("<hiveTable>").append(" ")
      .append("<selectedCols>").append(" ")
      .append("<timeColName>").append(" ")
      .append("<timeColPattern>").append(" ")
      .append("<timeRangeValue>").append(" ")
      .append("<timeRangeType>").append(" ")
      .append("<esIndexType>").append(" ")
      .append("<esIndexPre>").append(" ")
      .append("<esType>").append(" ")
      .append("<startTime>").append(" ")
      .append("<db>").append(" ")
    println(buff.toString())
  }
}
