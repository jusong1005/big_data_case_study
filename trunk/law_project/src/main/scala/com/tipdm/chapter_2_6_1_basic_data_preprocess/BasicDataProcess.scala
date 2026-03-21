package com.tipdm.chapter_2_6_1_basic_data_preprocess

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 基础数据预处理
  *
  * //@Author: fansy 
  * //@Time: 2019/2/12 14:21
  * //@Email: fansy1990@foxmail.com
  */
object BasicDataProcess {

  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      printUsage()
      System.exit(1)
    }
    //  0. 参数处理
    val (inputTable, outputTable, fullUrlColName, fullUrlTypeColName, appName) = handle_args(args)

    //  1. 初始化
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    // 2. 加载数据
    val data = sqlContext.read.table(inputTable)

    // 3. 去除重复数据
    val distinct_data = data.distinct()
    println("重复记录：" + (data.count() - distinct_data.count()))

    // 4. 翻页数据还原
    val next_page = udf { (page: String, url_type: String) => !is_next_page(page, url_type) }
    val revert_page = udf { (page: String, next_page: Boolean) =>
      if (!next_page) {
        val _split = page.lastIndexOf("_")
        page.substring(0, _split) + ".html"
      } else {
        page
      }
    }
    val handle_page_next_data = distinct_data.withColumn("next_page", next_page(col(fullUrlColName), col(fullUrlTypeColName)))
    println("翻页网页个数："+ handle_page_next_data.filter("next_page = false").count)
    val handled_page_next_data = handle_page_next_data.withColumn("url", revert_page(col(fullUrlColName), col("next_page")))
    print_next_page(handled_page_next_data, fullUrlColName)
    // 5. 还原被分享网页
    val revert_share_page = udf { (page: String, url_type: String) => {
      if (!("1999001".equals(url_type))) {
        if (page != null && page.contains("?")) {
          page.substring(0, page.indexOf("?"))
        } else {
          page
        }
      } else {
        page
      }
    }
    }
    val handled_share_data = handled_page_next_data.withColumn("url", revert_share_page(col("url"), col(fullUrlTypeColName)))
    print_share_data(handled_share_data, fullUrlColName, fullUrlTypeColName)
    // 6. 存储处理后数据
    handled_share_data.write.mode("overwrite").saveAsTable(outputTable)

    // 7. 关闭SparkContext
    sc.stop()

  }

  def print_share_data(handled_share_data: DataFrame, fullUrl: String, urlType: String) = {
    handled_share_data.filter(urlType + " != '1999001'").filter(fullUrl + " like '%?%'").select(fullUrl, "url").show(4, false)
  }

  def print_next_page(handled_page_next_data: DataFrame, url: String) = {
    handled_page_next_data.filter("next_page = false").select(substring(col(url), 30, 60), substring(col("url"), 30, 60)).show(10, false)
  }

  /**
    * 是否是翻页
    *
    * @param page
    * @param url_type
    * @return
    */
  def is_next_page(page: String, url_type: String): Boolean = {
    if (!("107001".equals(url_type)) || null == page || page.length < 6) {
      return false
    } else if (page.contains(".html") && page.contains("_")) {
      val second = page.lastIndexOf(".")
      val _before = page.lastIndexOf("_")
      if (_before >= 0 && second > _before + 1) {
        val t = page.substring(_before + 1, second)
        return small_int(t)
      }
    }
    return false
  }

  /**
    * 翻页最多只会有2位数
    *
    * @param num
    * @return
    */
  def small_int(num: String): Boolean = {
    if (StringUtils.isNumeric(num) && num.length < 3) true else false
  }

  /**
    * 处理参数
    *
    * @param args 输入参数
    * @return
    */
  def handle_args(args: Array[String]) = {
    val inputTable = args(0) // 输入数据
    val outputTable = args(1) // 数据输出
    val fullUrlColName = args(2) // 网页列
    val fullUrlTypeColName = args(3) // 网页类别列
    val appName = args(4) // 任务名
    (inputTable, outputTable, fullUrlColName, fullUrlTypeColName, appName)
  }

  /**
    * 使用说明
    */
  def printUsage(): Unit = {
    val buff = new StringBuilder
    buff.append("Usage : com.tipdm.chapter_2_6_1_basic_data_preprocess.BasicDataProcess").append(" ")
      .append("<inputTable>").append(" ")
      .append("<outputTable>").append(" ")
      .append("<fullUrlColName>").append(" ")
      .append("<fullUrlTypeColName>").append(" ")
      .append("<appName>").append(" ")
    println(buff.toString())
  }
}
