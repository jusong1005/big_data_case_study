package com.tipdm.chapter_2_9_2_table_recommend


import java.util.Properties

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 推荐结果写入MySQL
  * //@Author: fansy
  * //@Time: 2019/2/15 9:38
  * //@Email: fansy1990@foxmail.com
  */
object Result2MySQL {
  def main(args: Array[String]): Unit = {
    if (args.length != 7) {
      printUsage()
      System.exit(1)
    }
    //  0. 参数处理
    val (inputTableDB, inputTablePrefix, url, table, user, password, appName) = handle_args(args)

    //  1. 初始化
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    // 2. 拼接数据表
    val specificTables = sqlContext.sql("SHOW TABLES IN " + inputTableDB)
      .filter("tableName like '"+ inputTablePrefix +"%'").select("tableName")
      .rdd.map(row => row.getString(0)).collect()

    // 3. 整合数据
    val result = specificTables.map(t => sqlContext.read.table(inputTableDB + "." + t)).reduce((x, y) => x.unionAll(y))
    // 4. 写入MySQL
    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", password)
    result.write.mode(SaveMode.Overwrite).jdbc(url, table, properties)
    // 5. 关闭SparkContext
    sc.stop()

  }

  def printUsage() = {
    val buff = new StringBuilder
    buff.append("Usage : com.tipdm.chapter_2_9_2_table_recommend.Result2MySQL").append(" ")
      .append("<inputTableDB>").append(" ")
      .append("<inputTablePrefix>").append(" ")
      .append("<url>").append(" ")
      .append("<table>").append(" ")
      .append("<user>").append(" ")
      .append("<password>").append(" ")
      .append("<appName>").append(" ")
    println(buff.toString())
  }

  def handle_args(args: Array[String]) = {
    val inputTableDB = args(0)
    val inputTablePrefix = args(1)
    val url = args(2)
    val table = args(3)
    val user = args(4)
    val password = args(5)
    val appName = args(6)
    (inputTableDB, inputTablePrefix, url, table, user, password, appName)
  }

}
