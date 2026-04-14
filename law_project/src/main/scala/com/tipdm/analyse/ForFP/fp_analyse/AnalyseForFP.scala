package com.tipdm.analyse.ForFP.fp_analyse

import com.tipdm.analyse.ForPathKmeans.readOrwriteData.ReadHiveData
import com.tipdm.util.CommonUtil.{fullurl, userid, _}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{count, desc}

object AnalyseForFP {

  /**
    * 统计带？的网页
    *
    * @param data law_init1.lawtime_all
    */
  def analyseWenhao(data: DataFrame) = {
    val dataWithWenhao = data.filter(fullurl + " like '%?%'")
    dataWithWenhao
  }

  /**
    * 频数统计，统计每个用户访问的网页数，以及分布情况
    *
    * @param data
    */
  def analysePinshu(data: DataFrame) = {
    val count_userid = data.groupBy(userid).agg(count(fullurl) as "url_count").groupBy("url_count").agg(count("userid") as "user_count").orderBy(desc("url_count"))
    count_userid.show(10, false)
    println("用户访问的网页数共有" + count_userid.count() + "种")
  }

  /**
    * 统计翻页网址
    *
    * @param data
    * @return
    */
  def analyseFanye(data: DataFrame) = {
    val fanye = data.filter(fullurl + " like '%\\__.html%'")
    fanye
  }

  def main(args: Array[String]): Unit = {
    val data = ReadHiveData.readData(table_gt_oneclick_data_distinct)
    analyseWenhao(data).show(10, false)
  }
}
