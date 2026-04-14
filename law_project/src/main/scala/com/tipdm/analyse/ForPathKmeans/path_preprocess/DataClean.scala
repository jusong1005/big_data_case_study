package com.tipdm.analyse.ForPathKmeans.path_preprocess

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * suling
  * 数据清洗
  * 清洗规则：
  * （1）	去除网页总访问次数小于等于10的网页记录
  * （2）	去除同一个用户一次访问中的重复页面
  * （3）	过滤掉非HTML结尾的页面
  * （4）	过滤掉仅有一次访问记录的用户
  */
object DataClean {

  def cleanUrlCountLowTen(data: DataFrame, num: Int): DataFrame = {
    // 去除网页总访问次数小于等于n的网页记录
    val fullurl_gt_num = data.groupBy(fullurl).agg(count(fullurl) as "fcount").filter("fcount>" + num).select(fullurl).distinct()
    data.join(fullurl_gt_num, fullurl)
  }

  def cleanDuplicateAtOne(data: DataFrame): DataFrame = {
    // 过滤同一用户的一次访问中重复的页面
    data.orderBy(timestamp_format).dropDuplicates(Array(userid, fullurl))
  }

  def cleanNotHtml(data: DataFrame): DataFrame = {
    // 清除非HTML界面
    data.filter(fullurl + " like '%.html'")
  }

  def clean(data: DataFrame) = {
    val clean_lowten = cleanUrlCountLowTen(data, 10)
    val clean_duplicate = cleanDuplicateAtOne(clean_lowten)
    val clean_nothtml = cleanNotHtml(clean_duplicate)
    val clean_one = CleanOnlyOneClick.cleanOnlyOne(clean_nothtml, 1)
    clean_one
  }

  def main(args: Array[String]): Unit = {
    val data = ReadDB.getData()
    val clean_lowten = cleanUrlCountLowTen(data, 10)
    println("过滤访问低于10的网页后剩余记录：" + clean_lowten.count())
    val clean_duplicate = cleanDuplicateAtOne(clean_lowten)
    println("过滤重复后剩余：" + clean_duplicate.count())
    val clean_nothtml = cleanNotHtml(clean_duplicate)
    println("过滤非HTML剩余：" + clean_nothtml.count())
    val clean_one = CleanOnlyOneClick.cleanOnlyOne(clean_nothtml, 1)
    println("过滤一次访问后剩余：" + clean_one.count())
  }
}
