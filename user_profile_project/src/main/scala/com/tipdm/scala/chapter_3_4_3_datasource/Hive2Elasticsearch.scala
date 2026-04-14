package com.tipdm.scala.chapter_3_4_3_datasource

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql.EsSparkSQL

/**
  * //@Author:qwm
  * //@Date: 2018/8/16 16:50
  *
  * Hive数据导入Elasticsearch
  */

object Hive2Elasticsearch {
  def main(args: Array[String]): Unit = {
    if (args.length != 6) {
      printUsage()
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Hive2Elasticsearch")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val hiveTable = args(0)
    val esNode = args(1)
    val esPort = args(2)
    val timeColumn = args(3)
    val esIndex = args(4)
    val esType = args(5)
    val options = Map(
      ("es.nodes", esNode),
      ("es.port", esPort),
      ("es.index.auto.create", "true"),
      ("es.write.operation", "index")
    )
    if (hiveTable.contains("media")) {
      val weeks = sqlContext.sql("select distinct concat(year(" + timeColumn + "),'',weekofyear(" + timeColumn + ")) as week from "
        + hiveTable).rdd.map(row => row.getString(0)).collect()
      val data = sqlContext.sql("select *,concat(year(" + timeColumn + "),'',weekofyear(" + timeColumn + ")) as week from " + hiveTable)
      val table = "media_1d" + System.currentTimeMillis()
      data.registerTempTable(table)
      for (we <- weeks) {
        val data1 = sqlContext.sql("select * from " + table + " where week=" + we)
        val df = data1.drop("week")
        df.show(3)
        println(esIndex + "" + we + "/" + esType)
        EsSparkSQL.saveToEs(df, esIndex + "" + we + "/" + esType, options)
      }
    } else {
      val data = sqlContext.sql("select * from " + hiveTable)
      EsSparkSQL.saveToEs(data, esIndex + "/" + esType, options)
    }
    sc.stop()
  }
  /**
    * 使用说明
    */
  def printUsage(): Unit = {
    val buff = new StringBuilder
    buff.append("Usage : com.tipdm.scala.chatper_3_5_1_datasource.Hive2Elasticsearch").append(" ")
      .append("<hiveTable>").append(" ")
      .append("<esNode>").append(" ")
      .append("<esPort>").append(" ")
      .append("<timeColumn>").append(" ")
      .append("<esIndex>").append(" ")
      .append("<esType>").append(" ")
    println(buff.toString())
  }
}
