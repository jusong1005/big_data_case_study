package com.tipdm.analyse.ForPathKmeans.path_analyse

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * suling
  * 对网页标题类型与标题类型ID进行分析，看是否适合进行轨迹分析
  */
object AnalysePageTitle {

  /**
    * 统计用户访问的标题类型数分布
    *
    * @param data
    */
  def analysePageTitleAndId(data: DataFrame) = {
    val userNum = data.select(userid).distinct().count()
    data.groupBy(userid).agg(countDistinct("pagetitlecategoryid") as "pagecount").groupBy("pagecount").agg(count(userid) as "usercount", count(userid) * 100.0 / userNum as "user_percent").orderBy(desc("usercount"))
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
    //读取数据
    val data = sqlContext.sql(s"select * from $inputTable")
    val pid_distribute = analysePageTitleAndId(data)
    pid_distribute.show(20, false)
  }
}
