package com.tipdm.analyse

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * suling
  * 第一阶段数据探索
  */
object AnalyseForBasis {

  /**
    * 统计数据总量、用户数、网址数量
    *
    * @param data 原始数据记录
    * @return
    */
  def analyseDataNum(data: DataFrame) = {
    val num1 = data.count()
    val num2 = data.select(userid).distinct().count()
    val num3 = data.select(fullurl).distinct().count()
    println("数据总量：" + num1)
    println("用户数：" + num2)
    println("网址数量：" + num3)
    (num1, num2, num3)
  }

  /**
    * 统计用户月访问量和日访问量
    *
    * @param table lawtime_all
    */
  def analyseDayNum(table: String, dataNum: Long, sqlContext: HiveContext) = {
    val month_num = sqlContext.sql("select substring(timestamp_format,0,7) as ymd, count(substring(timestamp_format,0,7)) as ymd_count  from " + table + " group by substring(timestamp_format,0,7) order by substring(timestamp_format,0,7)")
    month_num.collect.map(x => x.getString(0) + "," + x.getLong(1) + "," + x.getLong(1) * 1.0 / dataNum).foreach(println(_))
    val day_num = sqlContext.sql("select substring(timestamp_format,0,10) as ymd, count(substring(timestamp_format,0,10)) as ymd_count  from " + table + " group by substring(timestamp_format,0,10) order by substring(timestamp_format,0,10)")
    day_num.collect.map(x => x.getString(0) + "," + x.getLong(1) + "," + x.getLong(1) * 1.0 / dataNum).foreach(println(_))
  }

  /**
    * 统计用户点击次数分布
    *
    * @param data    law_init1.lawtime_all
    * @param dataNum 记录数
    */
  def analyseClickNum(data: DataFrame, dataNum: Long) = {
    val userNum = data.select(userid).distinct().count()
    val clickCount = data.groupBy(userid).agg(count(userid) as "user_clicks")
      .groupBy("user_clicks").agg(count(userid) as "user_clicks_count", count(userid) * 100.0 / userNum as "user_per", count(userid) * 100.0 * col("user_clicks") / dataNum as "url_per")
      .select("user_clicks", "user_clicks_count", "user_per", "url_per").orderBy(desc("user_per"))
    clickCount.show(50, false)
  }

  /**
    * 探究用户访问记录跨越的天数分布情况
    *
    * @param data law_init1.lawtime_gt_one_distinct
    */
  def analyseVisitTime(data: DataFrame) = {
    val userNum = data.select(userid).distinct().count()
    val dcount = data.groupBy(userid).agg(countDistinct("ymd") as "dcount").groupBy("dcount").agg(count(userid) as "usercount", count(userid) * 100.0 / userNum as "userpercent").orderBy(desc("dcount"))
    dcount.show(20, false)
    val uid_avg = data.groupBy(userid).agg(col(userid), count(fullurl) / countDistinct("ymd") as "avgclick").groupBy("avgclick").agg(count(userid) as "ucount", count(userid) * 100.0 / userNum as "user_per").orderBy(desc("avgclick"))
    uid_avg.show(50, false)
  }

  /**
    * 用户记录去重并过滤点击次数在1以上的数据
    *
    * @param data law_init1.lawtime_all
    */
  def cleanDuplicateAndOne(data: DataFrame): DataFrame = {
    // 2.去重数据
    val distinct_data_userid = data.distinct()
    // 3. 去重后，访问次数大于1的用户
    val gt_one = distinct_data_userid.groupBy(userid).agg(count("userid") as "u_num").filter(("u_num >1")).select(col(userid) as "userid1")
    // 4. join两个数据
    distinct_data_userid.join(gt_one, distinct_data_userid(userid) === gt_one("userid1"), "inner")
  }

  /**
    * 统计重复记录数
    *
    * @param data law_init1.lawtime_all
    *
    */
  def analyseDruplicate(data: DataFrame) = {
    //val data=sqlContext.sql("select * from "+table)
    val c1 = data.count()
    val c2 = data.distinct().count()
    println("重复记录数：" + (c1 - c2))
  }

  def printUsage() = {
    val buff = new StringBuilder
    buff.append("Usage: com.tipdm.analyse.AnalyseForBasis ").append(" ")
      .append(" <appName> ")
      .append(" <inputTable> ")

  }

  def handle_args(args: Array[String]) = {
    val appName = args(0)
    val inputTable = args(1)
    (appName, inputTable)
  }

  def main(args: Array[String]): Unit = {
    val (appName, inputTable) = handle_args(args)
    val sc = new SparkContext(new SparkConf().setAppName(appName))
    val sqlContext = new HiveContext(sc)
    val data = sqlContext.sql(s"select * from $inputTable")
    val data_distinct_gt_one = cleanDuplicateAndOne(data)
    analyseVisitTime(data_distinct_gt_one)
  }
}
