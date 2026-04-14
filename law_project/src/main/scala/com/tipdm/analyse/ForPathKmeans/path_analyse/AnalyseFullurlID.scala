package com.tipdm.analyse.ForPathKmeans.path_analyse

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * suling
  * 对网页ID进行探索，查看是否适合进行轨迹分析
  */
object AnalyseFullurlID {
  /**
    * 统计每个用户访问的网页类型个数
    *
    * @param data table_gt_oneclick_data_distinct
    */
  def analyseUserClickID(data: DataFrame) = {
    val userNum = data.select(userid).distinct().count()
    data.groupBy(userid).agg(countDistinct(fullurlid) as "fid_count").groupBy("fid_count").agg(count(userid) as "usercount", count(userid) * 100.0 / userNum as "user_precent").orderBy(desc("usercount"))
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
    val fid_distribute = analyseUserClickID(data)
    fid_distribute.show(50, false)
  }
}
