package com.tipdm.dataclean

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * suling
  * 用于统计每个用户对每个网页的评分
  */
object DataPreprocess02_ALS {

  /**
    * 映射URL访问次数到评分
    * 规则：
    * 1~8  ：1~8
    * 9~23 ： 9
    * > 23 :  10
    *
    * @param times
    * @return
    */
  def trans_times_2_rate(times: Long): Double = {
    if (times <= 8) {
      times
    } else if (times > 23) {
      10
    } else {
      9
    }
  }

  /**
    * 统计访问次数并映射到评分
    *
    * @param data
    * @param userCol   用户
    * @param itemCol   网页
    * @param ratingCol 评分
    * @return
    */
  def mapClickToRating(data: DataFrame, userCol: String, itemCol: String, ratingCol: String) = {
    val times_2_rate = udf { (times: Long) => trans_times_2_rate(times) }
    val trainRdd = data.select(userCol, itemCol).groupBy(userCol, itemCol).agg(count(lit(1)) as ratingCol)
    trainRdd
  }

  def parseArgs(args: Array[String]) = {
    // 输入表,输出表，用户id列、网址id列，评分列
    (args(0), args(1), args(2), args(3), args(4))
  }

  def main(args: Array[String]): Unit = {
    val (inputTable, outTable, usercol, itemcol, ratingcol) = parseArgs(args)
    // 初始化
    val sc = new SparkContext(new SparkConf().setAppName("datapreprocss als"))
    val sqlContext = new HiveContext(sc)
    // 读取数据
    val data = sqlContext.sql("select * from " + inputTable)
    val data_with_rating = mapClickToRating(data, usercol, itemcol, ratingcol)
    saveHiveTable(sqlContext, data_with_rating, outTable, true)
  }
}
