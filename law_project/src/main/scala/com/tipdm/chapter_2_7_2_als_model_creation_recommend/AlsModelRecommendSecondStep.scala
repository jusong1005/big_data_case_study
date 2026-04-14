package com.tipdm.chapter_2_7_2_als_model_creation_recommend

import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 根据参数，利用分割的ALS模型进行推荐
  * //@Author: fansy 
  * //@Time: 2019/2/18 13:54
  * //@Email: fansy1990@foxmail.com
  */
object AlsModelRecommendSecondStep {

  def main(args: Array[String]): Unit = {
    if (args.length != 12) {
      printUsage()
      System.exit(1)
    }
    //  0. 参数处理
    val (modelTable, useridCol, urlidCol, ratingCol,
    userMetaTable, userCol, urlMetaTable, urlCol, recNum, recTable, partitionSize, appName) = handle_args(args)

    //  1. 初始化
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    // 2. 加载并缓存模型
    val model = MatrixFactorizationModel.load(sc, modelTable)
    val userFeatures_new = model.userFeatures.partitionBy(new org.apache.spark.HashPartitioner(partitionSize))
    val allProductFeatures = model.productFeatures.partitionBy(new org.apache.spark.HashPartitioner(partitionSize))

    userFeatures_new.cache
    allProductFeatures.cache

    val new_model = new MatrixFactorizationModel(model.rank, userFeatures_new, allProductFeatures)

    // 3. 进行推荐
    val recommend: RDD[(Int, Array[Rating])] = new_model.recommendProductsForUsers(recNum)
    //    println(new Date() + ": finished recommend, " + recommend.count())

    // 4. 加载编码元数据，并转换, 使用join进行反编码
    val userMeta = sqlContext.read.table(userMetaTable).select(userCol, useridCol)
    val urlMeta = sqlContext.read.table(urlMetaTable).select(urlCol, urlidCol)
    import sqlContext.implicits._
    val recommendDf = recommend.flatMap(record => record._2).toDF
    val result = recommendDf.join(userMeta, recommendDf("user") === userMeta(useridCol), "left")
      .select(userCol, "product", "rating")
      .join(urlMeta, recommendDf("product") === urlMeta(urlidCol), "left")
      .select(userCol, urlCol, "rating")

    // 5. 存储推荐数据
    result.write.mode(SaveMode.Overwrite).saveAsTable(recTable)

    // 6. 关闭SparkContext
    sc.stop()
  }

  /**
    * 处理参数
    *
    * @param args 输入参数
    * @return
    */
  def handle_args(args: Array[String]) = {
    val inputTable = args(0) // 输入建模数据
    val useridCol = args(1) // 输入用户编码列名
    val urlidCol = args(2) // 输入网页编码列名
    val ratingCol = args(3) // 输入网页评分列名
    val userMetaTable = args(4) // 输入用户元数据编码表
    val userCol = args(5) // 输入用户元数据编码表 用户列名
    val urlMetaTable = args(6) // 输入网页元数据编码表
    val urlCol = args(7) // 输入网页元数据编码表 网页列名
    val recNum = args(8).toInt // 推荐个数
    val recTable = args(9) // 推荐结果存储表
    val partitionSize = args(10).toInt // 分区数
    val appName = args(11) // 任务名
    (inputTable, useridCol, urlidCol, ratingCol,
      userMetaTable, userCol, urlMetaTable, urlCol, recNum, recTable, partitionSize, appName)
  }

  /**
    * 使用说明
    */
  def printUsage(): Unit = {
    val buff = new StringBuilder
    buff.append("Usage : com.tipdm.chapter_2_7_2_als_model_creation_recommend.AlsModelCreationAndRecommend").append(" ")
      .append("<inputTable>").append(" ")
      .append("<useridCol>").append(" ")
      .append("<urlidCol>").append(" ")
      .append("<ratingCol>").append(" ")
      .append("<userMetaTable>").append(" ")
      .append("<userCol>").append(" ")
      .append("<urlMetaTable>").append(" ")
      .append("<urlCol>").append(" ")
      .append("<recNum>").append(" ")
      .append("<recTable>").append(" ")
      .append("<partitionSize>").append(" ")
      .append("<appName>").append(" ")
    println(buff.toString())
  }

}
