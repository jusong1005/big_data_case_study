package com.tipdm.analyse.ForPathKmeans.path_preprocess

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{collect_set, concat_ws}

/**
  * suling
  * 将每个用户的访问网址按照时间排序后出串联
  */
object ConnectURL {

  def concat_url(data: DataFrame, col: String, newCol: String, separator: String): DataFrame = {
    // 将同一个用户的所有轨迹点串成一条轨迹
    data.groupBy(userid).agg(concat_ws(separator, collect_set(col)) as newCol).select(userid, newCol)
  }

  def main(args: Array[String]): Unit = {
    val sqlContext = getSparkSession("connect fullurl")
    val data = ReadDB.getData(sqlContext)
    val data_concat = concat_url(data, fullurl, "fullurls", "\\|\\|\\|")
    data_concat.show(10)
  }
}
