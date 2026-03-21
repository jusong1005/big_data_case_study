package com.tipdm.analyse.ForPathKmeans.path_analyse

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object AnalyseForKmeans {

  /**
    * 统计每个网页被访问的次数的分布情况
    *
    * @param data law_init1.lawtime_gt_one_distinct
    */
  def analyseWebBeVisited(data: DataFrame) = {
    val webNum = data.select(fullurl).distinct().count()
    println("网页个数为：" + webNum)
    val webDistribute = data.groupBy(fullurl).agg(count(userid) as "webCount").groupBy("webCount").agg(count(fullurl) as "count", count(fullurl) * 100.0 / webNum as "count_percent").orderBy(desc("count"))
    webDistribute
  }

  /**
    * 统计每个网页被访问的次数
    *
    * @param data
    * @return
    */
  def analyseWebClickNum(data: DataFrame) = {
    data.groupBy(fullurl).agg(count(fullurl) as "urlCount") //.orderBy(desc("urlCount"))
  }

  /**
    * 统计被访问次数在10以内的网页个数
    *
    * @param data
    * @param num
    * @return
    */
  def analyseUrlUserlowTen(data: DataFrame, num: Int) = {
    data.filter("urlCount<=" + num)
  }

  /**
    * 统计用户点击次数分布
    *
    * @param data law_init1.lawtime_gt_one_distinct
    */
  def analyseClickNum(data: DataFrame) = {
    val userNum = data.select(userid).distinct().count()
    val dataNum = data.count()
    val clickCount = data.groupBy(userid).agg(count(userid) as "user_clicks")
      .groupBy("user_clicks").agg(count(userid) as "user_clicks_count", count(userid) * 100.0 / userNum as "user_per", count(userid) * 100.0 * col("user_clicks") / dataNum as "url_per")
      .select("user_clicks", "user_clicks_count", "user_per", "url_per").orderBy(desc("user_per"))
    clickCount.show(50, false)

  }

  /**
    * 统计每个用户点击的网页数law_init.lawtime_gt_one_distinct
    *
    * @param data
    */
  def analyseUserClickUrlNum(data: DataFrame) = {
    val userNum = data.select(userid).distinct().count()
    val data2 = data.groupBy(userid).agg(countDistinct(fullurlid) as "fcount").groupBy("fcount").agg(count(userid) as "ucount", count(userid) * 100.0 / userNum as "user_per").orderBy(desc("ucount"))
    data2.show(100, false)
  }

  def parseArgs(args: Array[String]) = {
    val appName = args(0)
    val inputTable = args(1)
    (appName, inputTable)
  }

  def main(args: Array[String]): Unit = {
    val (appName, inputTable) = parseArgs(args)
    //初始化
    val sc = new SparkContext(new SparkConf().setAppName(appName))
    val sqlContext = new HiveContext(sc)
    val data = sqlContext.sql(s"select * from $inputTable")
    val webClickNum = analyseWebClickNum(data)
    analyseUrlUserlowTen(webClickNum, 1).show(false)
    println("网页被点击次数低于10的网页数：" + analyseUrlUserlowTen(webClickNum, 10).count())
    println("网页被点击次数低于10的网页对应的记录数：" + data.join(analyseUrlUserlowTen(webClickNum, 10), fullurl).count())
    analyseUserClickUrlNum(data)
    analyseClickNum(data)
  }
}
