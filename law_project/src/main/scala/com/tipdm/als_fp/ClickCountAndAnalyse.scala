package com.tipdm.als_fp

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object ClickCountAndAnalyse {

  /**
    * 计算用户对于每个页面的访问次数的分布情况
    *
    * @param data
    */
  def analyseClick(data: DataFrame) = {
    val allUserURLSize = data.select("uid", "pid").distinct.count
    data.select("uid", "pid").groupBy("uid", "pid").agg(count(lit(1)) as "u_p_count").select("u_p_count")
      .groupBy("u_p_count").agg(count(lit(1)) as "u_p_count_num", count(lit(1.0)) / allUserURLSize as "u_p_count_num_percent")
      .orderBy(desc("u_p_count_num")).show(100, false)
  }

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("analyse click"))
    val sqlContext = new HiveContext(sc)
    val data = sqlContext.sql("select * from law_init1.data_101003_encoded")
    analyseClick(data)

  }
}
