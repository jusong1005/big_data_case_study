package com.tipdm.recommendsave

import java.util.Properties

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SaveMode, _}
import org.apache.spark.{SparkConf, SparkContext}

object RecommendSave_MYSQL {
  def writeData(data: DataFrame, table: String, database: String, ip: String) = {
    val user = "root"
    val password="123456"
    val driver = "com.mysql.jdbc.Driver"
    val saveMode = SaveMode.Overwrite
    val url = "jdbc:mysql://" + ip + ":3306/" + database + "?useUnicode=true&characterEncoding=utf8"

    val properties = new Properties()
    properties.put("user", user)
    properties.put("password", password)
    properties.put("driver", driver)
    data.write.mode(saveMode).jdbc(url, table, properties)
  }


  def sampleLength(array: Array[String], recNum: Int) = {
    var arr = Array.fill(recNum)("null")
    for (i <- 0 until array.length) {
      arr(i) = array(i)
    }
    arr
  }

  def aaa(columnsName: Array[String], colName: String) = {
    val arr = new Array[Column](columnsName.length)
    arr(0) = col(columnsName(0))
    for (i <- 1 until columnsName.length) {
      arr(i) = col({
        colName
      }).getItem({
        i - 1
      }).as({
        columnsName(i)
      })
    }
    arr
  }

  def parseArgs(args: Array[String]) = {
    (args(0), args(1), args(2), args(3), args(4), args(5).toInt, args(6).split(","))
  }

  def main(args: Array[String]): Unit = {
    val (appName, recommendPath, table, database, ip, recNum, columnsName) = parseArgs(args)
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    val data = sc.textFile(recommendPath).map { x => val xx = x.split("\\|\\|\\|"); (xx(0), xx.slice(1, xx.length)) }.filter(_._2.length > 0).mapValues(x => sampleLength(x, recNum)).mapValues(x => x.mkString("|||")).toDF(columnsName(0), "recommends")
    val data_recommend = data.withColumn("splitcol", split(col("recommends"), "\\|\\|\\|"))
    val arr = aaa(columnsName, "splitcol")
    val ddd = data_recommend.select(arr: _*)
    writeData(ddd, table, database, ip)
  }
}
