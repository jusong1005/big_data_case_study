package com.tipdm.als_fp

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

/**
  * 对选出来的咨询数据进行去重，去重后重新过滤一次用户
  */
object dataprocess {
  /**
    *
    * @param data   law_init1.data_101003_url_gt_one
    * @param sqlContext
    * @param output law_init1.data_101003_real_url_gt_one
    * @return
    */
  def processOnlyOneClick(data: DataFrame, sqlContext: HiveContext, output: String) = {
    data.select(col("userid"), struct(col("url"), col("timestamp_format")) as "url_ts").distinct
      .groupBy("userid").agg(collect_list("url_ts") as "url_list").filter("size(url_list) > 1")
      .withColumn("url_list", explode(col("url_list")))
      .select(col("userid"), col("url_list.url"), col("url_list.timestamp_format")).registerTempTable("u0")

    sqlContext.sql("create table " + output + " as select * from u0")
  }

  def main(args: Array[String]): Unit = {
    val sqlContext = getSparkSession("preprocess data for Als")
    val data = sqlContext.sql("select * from law_init1.data_101003_url_gt_one")
    processOnlyOneClick(data, sqlContext, data_101003_real_url_gt_one)
  }
}
