package com.tipdm.analyse

import com.tipdm.util.CommonUtil.{saveHiveTable, userid}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object DataProcessFirst {
  /**
    * 这个方法的作用是根据输入的数据以及字段名称去重
    *
    * @param data    输入的数据DataFrame
    * @param columns 进行记录间重复值匹配的字段
    * @return 返回的是去重后的DataFrame
    */
  def cleanDuplication(data: DataFrame, columns: Array[String]) = {
    if (columns != null && columns.length > 0) {
      data.dropDuplicates(columns)
    }
    else {
      data.dropDuplicates()
    }
  }

  /**
    * 这个方法的作用是过滤点击次数低于num的用户记录
    *
    * @param data
    * @param num
    */
  def cleanClickLowNum(data: DataFrame, num: Int) = {
    val gt_one_data_userid = data.groupBy(userid).agg(count(userid) as "num").filter("num>" + num).select(userid)
    // 所有数据和访问次数大于1的记录做内连接，并注册临时表
    data.join(gt_one_data_userid, userid)
  }

  def printUsage() = {
    val buff = new StringBuilder
    buff.append("Usage: com.tipdm.analyse.DataProcessFirst ").append(" ")
      .append(" <inputTable> ")
      .append(" <output01> ")
  }

  def parseArgs(args: Array[String]) = {
    val inputTable = args(0)
    val output01 = args(1)
    (inputTable, output01)
  }

  def main(args: Array[String]): Unit = {
    val (inputTable, output01) = parseArgs(args)
    val sc = new SparkContext(new SparkConf().setAppName("datapreprocss first"))
    val sqlContext = new HiveContext(sc)
    //读取原始数据
    val data = sqlContext.sql("select * from " + inputTable) //保存增量数据的Hive表
    // 进行重复数据跟访问一次的用户记录处理，处理后的数据在保存在Hive表law_init1.lawtime_gt_one_distinct中
    val data_to_duplication = cleanDuplication(data, null)
    val data_to_oneClick = cleanClickLowNum(data_to_duplication, 1).select("realip", "realareacode", "useragent", "useros", "userid", "clientid", "timestamps", "timestamp_format", "pagepath", "ymd", "fullurl", "fullurlid", "hostname", "pagetitle", "pagetitlecategoryid", "pagetitlecategoryname", "pagetitlekw", "fullreferrrer", "fullreferrerurl", "organickeyword", "source")
    saveHiveTable(sqlContext, data_to_oneClick, output01, true)
  }
}
