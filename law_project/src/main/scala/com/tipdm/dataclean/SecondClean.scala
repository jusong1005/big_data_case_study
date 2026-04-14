package com.tipdm.dataclean

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory

object SecondClean {
  val logger = LoggerFactory.getLogger(SecondClean.getClass)

  /**
    * 选择咨询相关数据
    *
    * @param table law_init1.lawtime_gt_one_distinct
    * @return 符合要求的fullURL
    */
  def getAskData(table: String, sqlContext: HiveContext) = {
    // 全部数据
    val data = sqlContext.sql("select * from " + table)
    // law_init1.lawtime_gt_one_distinct
    // 101003的所有数据
    val data_101003 = data.filter("fullurlid = 101003")
    data_101003
  }

  /**
    * 处理网址中的？
    *
    * @param data   咨询类别数据data_101003
    * @param output 结果存放表law_init1.data_101003_processed
    * @return
    */
  def fiterWenhao(data: DataFrame, output: String, sqlContext: HiveContext) = {
    sqlContext.udf.register("getRealUrl", (url: String) => if (url.contains("?")) url.substring(0, url.indexOf("?")) else url)
    data.selectExpr("getRealUrl(fullurl) as url", "userid", "timestamp_format").registerTempTable("a1")
    sqlContext.sql("drop table if exists " + output)
    sqlContext.sql("create table " + output + " as select * from a1")
  }

  /**
    * 过滤一次访问用户记录
    *
    * @param input  咨询类别数据law_init1.data_101003_processed
    * @param output law_init1.data_101003_url_gt_one
    * @return
    */
  def cleanClickOne(input: String, output: String, sqlContext: HiveContext) = {
    val data = sqlContext.sql("select * from " + input)
    val url_count_gt_one_users = data.groupBy("userid").agg(count("url") as "url_count").filter("url_count > 1").select("userid").distinct
    data.join(url_count_gt_one_users, "userid").registerTempTable("a2")
    sqlContext.sql("drop table if exists " + output)
    sqlContext.sql("create table " + output + " as select * from a2")
  }

  /**
    * 先去重再过滤一次访问用户记录
    *
    * @param input  law_init1.data_101003_url_gt_one
    * @param output law_init1.data_101003_url_gt_one_distinct
    */
  def cleanDuplicate(input: String, output: String, sqlContext: HiveContext): Unit = {
    val data = sqlContext.sql("select * from " + input).distinct.groupBy("userid").agg(collect_set("url") as "url_sets").filter("size(url_sets) > 1")
    data.registerTempTable("a3")
    sqlContext.sql("drop table if exists " + output)
    sqlContext.sql("create table " + output + " as select * from a3")
  }

  def main(args: Array[String]): Unit = {
    val sqlContext = getSparkSession("SecondClean")
    val data_101003 = getAskData(args(0), sqlContext)
    fiterWenhao(data_101003, args(1), sqlContext)
    cleanClickOne(args(1), args(2), sqlContext)
    cleanDuplicate(args(2), args(3), sqlContext)
  }
}
