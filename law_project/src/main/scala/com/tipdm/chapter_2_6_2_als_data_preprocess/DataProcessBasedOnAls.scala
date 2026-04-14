package com.tipdm.chapter_2_6_2_als_data_preprocess

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

/**
  * ALS模型主题的数据预处理
  */
object DataProcessBasedOnAls {
  val URL_TS = "url_ts"
  val URL_LIST = "url_list"
  val URL_TS_LIST = "url_ts_list"
  val RATING = "rating"

  /**
    * 过滤访问一次的用户， 并把访问次数映射为分数
    * @param encoded_data
    * @param useridColName
    * @param urlidColName
    * @param timeColName
    * @return
    */
  def filter_construct_model_data(encoded_data: DataFrame,  timeColName:String,
                                  useridColName:String, urlidColName:String):DataFrame = {
    val get_rate = udf{(frequency:Int ) => trans_times_2_rate(frequency)}
    val get_first = udf{(arr:scala.collection.mutable.WrappedArray[Int]) => arr.apply(0)}
    encoded_data.select(col(useridColName), struct(col(urlidColName), col(timeColName)) as URL_TS).distinct
      .groupBy(useridColName).agg(collect_list(URL_TS) as URL_TS_LIST).filter("size(" + URL_TS_LIST + ") > 1")
      .withColumn(URL_TS_LIST, explode(col(URL_TS_LIST)))
      .select(col(useridColName), col(URL_TS_LIST+"." +urlidColName))
        .groupBy(useridColName, urlidColName).agg(lit(1) as RATING)
      .withColumn(RATING, get_rate(col(RATING)))
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 14) {
      printUsage()
      System.exit(1)
    }


    //  0. 参数处理
    val (output_model_data, inputTable, train_validate_percent, modelTrainTable,
    validateTrainTable, validateTable,userMetaTable, urlMetaTable,
    userColName, urlColName, timeColName, useridColName, urlidColName , appName) = handle_args(args)

    //  1. 初始化
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    // 2. 加载输入数据
    val data = sqlContext.read.table(inputTable).select(userColName, urlColName,timeColName)


    // 3. 用户列及网页列 编码
    import sqlContext.implicits._
    val allUsers= data.select(userColName).distinct().orderBy(userColName).rdd.map(_.getString(0)).zipWithIndex().map(x => User_ID(x._1,x._2.toInt)).toDF
    val allItems = data.select(urlColName).distinct().orderBy(urlColName).rdd.map(_.getString(0)).zipWithIndex().map(x => Item_ID(x._1,x._2.toInt)).toDF

    val encoded_data = data.join(allUsers,data(userColName) === allUsers("userid"),"leftouter").drop(allUsers("userid")).join(allItems,data(urlColName) === allItems("fullurl"),"leftouter").drop(allItems("fullurl"))

    if(output_model_data){// 只输出建模数据、 编码元数据
      // 4. 过滤 并构建 建模数据
      val result = filter_construct_model_data(encoded_data, timeColName,useridColName,urlidColName)

      // 5. 构建用户列、网页列编码元数据

      val userMeta = allUsers
      val urlMeta = allItems

      // 6. 保存 建模数据/编码元数据
      result.write.mode(SaveMode.Overwrite).saveAsTable(modelTrainTable)
      userMeta.write.mode(SaveMode.Overwrite).saveAsTable(userMetaTable)
      urlMeta.write.mode(SaveMode.Overwrite).saveAsTable(urlMetaTable)


    }else{// 输出用于模型参数寻优的训练、测试表
      val validate_data = encoded_data
      val num = validate_data.count
      val split_point = (num * train_validate_percent(0) / train_validate_percent.sum).toInt
      // 得到分割点
      val split_time = validate_data.select(timeColName).orderBy(timeColName).rdd.map(row => row.getString(0))
        .zipWithIndex.filter(x => x._2 == split_point).collect.apply(0)
      //保存建模数据
      val validata_train = validate_data.filter(timeColName + " < '" + split_time._1 + "'")
      filter_construct_model_data(validata_train, timeColName,useridColName,urlidColName).write.mode(SaveMode.Overwrite).saveAsTable(validateTrainTable)
      //保存测试数据
      val validate = validate_data.filter(timeColName + " >= '" + split_time._1 + "'")
      validate.write.mode(SaveMode.Overwrite).saveAsTable(validateTable)
    }
    //  关闭SparkContext
    sc.stop()

  }

  /**
    * 映射URL访问次数到评分
    * 规则：
    * 1~3  ：1~8
    * 9~23 ： 9
    * > 23 :  10
    */

  def trans_times_2_rate(times: Int): Double = {
    if (times <= 8) {
      times
    } else if (times > 23) {
      10
    } else {
      9
    }
  }

  /**
    * 处理参数
    *
    * @param args 输入参数
    * @return
    */
  def handle_args(args: Array[String]) = {
    val output_model_data = args(0).toBoolean // 输出建模数据、用户网页元数据或模型寻优数据
    val inputTable = args(1) // 输入数据
    val train_validate_percent = args(2).split(",").map(_.toDouble) // 训练、验证集比例，逗号分隔
    val modelTrainTable = args(3) // 建模数据输出
    val validateTrainTable = args(4) // 模型寻优建模数据输出
    val validateTable = args(5) // 模型寻优验证数据输出
    val userMetaTable = args(6) // 用户列元数据
    val urlMetaTable = args(7) // 网页列元数据
    val userColName = args(8) // 用户ID列
    val urlColName = args(9) // 网页列
    val timeColName = args(10) // 时间列
    val useridColName = args(11) // 用户列编码后ID列名
    val urlidColName = args(12) // 网页列编码后ID列名

    val appName = args(13) // 任务名
    (output_model_data, inputTable, train_validate_percent, modelTrainTable, validateTrainTable, validateTable,
      userMetaTable, urlMetaTable, userColName, urlColName, timeColName,useridColName,urlidColName, appName)
  }

  /**
    * 使用说明
    */
  def printUsage(): Unit = {
    val buff = new StringBuilder
    buff.append("Usage : com.tipdm.chapter_2_6_2_als_data_preprocess.DataProcessBasedOnAls").append(" ")
      .append("<output_model_data>").append(" ")
      .append("<inputTable>").append(" ")
      .append("<train_percent,validate_percent>").append(" ")
      .append("<modelTrainTable>").append(" ")
      .append("<validateTrainTable>").append(" ")
      .append("<validateTable>").append(" ")
      .append("<userMetaTable>").append(" ")
      .append("<urlMetaTable>").append(" ")
      .append("<userColName>").append(" ")
      .append("<urlColName>").append(" ")
      .append("<timeColName>").append(" ")
      .append("<useridColName>").append(" ")
      .append("<urlidColName>").append(" ")
      .append("<appName>").append(" ")
    println(buff.toString())
  }

  case class Item_ID(fullurl: String, pid: Int)
  case class User_ID(userid: String, uid: Int)
}