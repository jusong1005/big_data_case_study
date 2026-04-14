package com.tipdm.chapter_2_6_3_fp_data_preprocess

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{col, collect_set, size}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * 关联规则主题的数据预处理
  *
  * //@Author: fansy 
  * //@Time: 2019/1/12 11:07
  * //@Email: fansy1990@foxmail.com
  */
object DataProcessBasedOnFp {

  val URL_SETS_COLUMN = "url_sets_column"

  def main(args: Array[String]): Unit = {
    if (args.length != 10) {
      printUsage()
      System.exit(1)
    }
    //  0. 参数处理
    val (output_model_data, inputTable, train_validate_percent, modelTrainTable,
    validateTrainTable, validateTable, uidColName, urlColName, timeColName, appName) = handle_args(args)

    //  1. 初始化
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    // 2. 加载数据
    val data = sqlContext.read.table(inputTable)

    if (output_model_data) {
      // 3. 过滤得到建模表
      data.select(uidColName, urlColName).distinct.groupBy(uidColName).agg(collect_set(col(urlColName)) as URL_SETS_COLUMN)
        .filter(size(col(URL_SETS_COLUMN)) > 1).select(URL_SETS_COLUMN).write.mode(SaveMode.Overwrite).saveAsTable(modelTrainTable)
    } else {
      // 3. 得到模型寻优数据
      val validate_data = data.select(uidColName, urlColName, timeColName).distinct
      val num = validate_data.count
      val split_point = (num * train_validate_percent(0) / train_validate_percent.sum).toInt
      // 得到分割点
      val split_time = validate_data.select(timeColName).orderBy(timeColName).rdd.map(row => row.getString(0))
        .zipWithIndex.filter(x => x._2 == split_point).collect.apply(0)
      // 保存建模数据
      validate_data.filter(timeColName + " < '" + split_time._1 + "'").select(uidColName, urlColName).distinct
        .groupBy(uidColName).agg(collect_set(col(urlColName)) as URL_SETS_COLUMN).filter(size(col(URL_SETS_COLUMN)) > 1)
        .select(URL_SETS_COLUMN).write.mode(SaveMode.Overwrite).saveAsTable(validateTrainTable)
      // 保存测试数据
      validate_data.filter(timeColName + " >= '" + split_time._1 + "'").write.mode(SaveMode.Overwrite).saveAsTable(validateTable)
    }

    // 4. 关闭SparkContext
    sc.stop()

  }

  /**
    * 处理参数
    * @param args 输入参数
    * @return
    */
  def handle_args(args: Array[String]) = {
    val output_model_data = args(0).toBoolean // 输出建模数据或模型寻优数据
    val inputTable = args(1) // 输入数据
    val train_validate_percent = args(2).split(",").map(_.toDouble) // 训练、验证集比例，逗号分隔
    val modelTrainTable = args(3) // 建模数据输出
    val validateTrainTable = args(4) // 模型寻优建模数据输出
    val validateTable = args(5) // 模型寻优验证数据输出
    val uidColName = args(6) // 用户ID列
    val urlColName = args(7) // 网页列
    val timeColName = args(8) // 时间列
    val appName = args(9) // 任务名
    (output_model_data, inputTable, train_validate_percent, modelTrainTable, validateTrainTable, validateTable,
      uidColName, urlColName, timeColName, appName)
  }

  /**
    * 使用说明
    */
  def printUsage(): Unit = {
    val buff = new StringBuilder
    buff.append("Usage : com.tipdm.chapter_2_6_3_fp_data_preprocess.DataProcessBasedOnFp").append(" ")
      .append("<output_model_data>").append(" ")
      .append("<inputTable>").append(" ")
      .append("<train_percent,validate_percent>").append(" ")
      .append("<modelTrainTable>").append(" ")
      .append("<validateTrainTable>").append(" ")
      .append("<validateTable>").append(" ")
      .append("<uidColName>").append(" ")
      .append("<urlColName>").append(" ")
      .append("<timeColName>").append(" ")
      .append("<appName>").append(" ")
    println(buff.toString())
  }
}
