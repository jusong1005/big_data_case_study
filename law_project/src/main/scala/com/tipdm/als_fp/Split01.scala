package com.tipdm.als_fp

import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer


/**
  * 数据分割，按照某一列排序，以及提供的比例
  * //@Author: fansy 
  * //@Time: 2018/10/24 11:36
  * //@Email: fansy1990@foxmail.com
  */
object Split01 {
  val log = LoggerFactory.getLogger(Split01.getClass)

  /**
    * 按照 sortColumn的顺序排序，并按照percents里面的比例分割数据
    *
    * @param spark
    * @param data
    * @param percents
    * @param sortColumn
    * @param sortColumnIsString
    * @return
    */
  def split(spark: SparkSession, data: DataFrame, percents: Array[Double], sortColumn: String, sortColumnIsString: Boolean): Array[DataFrame] = {
    // 1. 获取分割点
    val all_size = data.count()
    val real_precents = percents.map(x => (x / percents.sum * all_size).toInt)
    if (sortColumnIsString) {
      val allSortedData = data.select(sortColumn).orderBy(sortColumn).rdd.map(_.getString(0)).zipWithIndex()
      var sum_count = real_precents
      val result = new ArrayBuffer[DataFrame]()
      for (i <- 1 until real_precents.size) {
        // result.
      }
    } else {
      val allSortedData = data.select(sortColumn).orderBy(sortColumn).rdd.map(_.getDouble(0)).zipWithIndex()
    }

    ???
  }


  /**
    * 分割为训练、测试集
    *
    * @param data
    * @param trainPercent
    * @param testPercent
    * @param sortedColumn
    * @return
    */
  def split2(data: DataFrame, trainPercent: Double, testPercent: Double, sortedColumn: String): (DataFrame, DataFrame) = {
    val (d1, d2, d3) = split3(data, trainPercent, testPercent, 0.0, sortedColumn)
    (d1, d2)
  }

  /**
    * 分割训练、验证、测试集
    *
    * @param data
    * @param trainPercent
    * @param validatePercent
    * @param testPercent
    * @param sortedColumn
    * @return
    */
  def split3(data: DataFrame, trainPercent: Double, validatePercent: Double, testPercent: Double, sortedColumn: String): (DataFrame, DataFrame, DataFrame) = {
    val percents = Array(trainPercent, validatePercent, testPercent)
    val all_size = data.count()
    val real_precents = percents.map(x => (x / percents.sum * all_size).toLong)
    val allSortedColumnData = data.select(sortedColumn).orderBy(sortedColumn).rdd.map(_.getString(0)).zipWithIndex().map(x => (x._2, x._1))
    val firstSplitPoint = allSortedColumnData.lookup(real_precents(0)).head
    if (testPercent >= 0.0) {
      val secondSplitPoint = allSortedColumnData.lookup(real_precents(0) + real_precents(1)).head
      (
        data.filter(sortedColumn + " < '" + firstSplitPoint + "'"),
        data.filter(sortedColumn + " >= '" + firstSplitPoint + "' and " + sortedColumn + " < '" + secondSplitPoint + "'"),
        data.filter(sortedColumn + " >= '" + secondSplitPoint + "'")
      )
    } else {
      (
        data.filter(sortedColumn + " < " + firstSplitPoint),
        data.filter(sortedColumn + " >= " + firstSplitPoint),
        null
      )
    }
  }

  def main(args: Array[String]): Unit = {
    val appName = getClass.toString
    // 1. 初始化
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 2. 读取数据
    val data = sqlContext.sql("select * from law_init1.data_101003_encoded")

    //3.数据拆分
    val (train, validate, test) = split3(data, 0.8, 0.1, 0.1, "timestamp_format")

    //4.存储
    train.registerTempTable("train00")
    validate.registerTempTable("validate00")
    test.registerTempTable("test00")

    sqlContext.sql("create table law_init1.data_101003_encoded_train as select * from train00")
    sqlContext.sql("create table law_init1.data_101003_encoded_validate as select * from validate00")
    sqlContext.sql("create table law_init1.data_101003_encoded_test as select * from test00")
  }
}
