package com.tipdm.chapter_2_7_2_als_model_creation_recommend
import java.util.Properties
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
/**
  * 根据参数，构建ALS模型，分割模型并分别存储
  */
object AlsModelCreationFirstStep {
  val ts = "ts"
  val SMALL_SIZE = 40960
  def main(args: Array[String]): Unit = {
    if (args.length != 10) {
      printUsage()
      System.exit(1)
    }
    //  0. 参数处理
    val (inputTable, useridCol, urlidCol, ratingCol,
    url, table, user, password,
    modelTable, appName) = handle_args(args)
    //  1. 初始化
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    // 加载模型参数
    val alsParam = getModelArgs(sqlContext, url, table, user, password)
    // 2. 加载数据，并转换为Rating数据
    val data = sqlContext.read.table(inputTable).select(useridCol, urlidCol, ratingCol)
      .rdd.map(row => Rating(row.getInt(0), row.getInt(1), row.getDouble(2)))
    data.cache()
    // 3. 建立模型
    val model = if (alsParam.bestImplicitPref) {
      ALS.trainImplicit(data, alsParam.bestRank, alsParam.bestIter, alsParam.bestReg, alsParam.bestAlphas)
    } else {
      ALS.train(data, alsParam.bestRank, alsParam.bestIter, alsParam.bestReg)
    }
    data.unpersist()// unpersist this data
    // 4. 按照SMALL_SIZE分割model.userFeatures
    val userFeatures = model.userFeatures
    val productFeatures = model.productFeatures
    val  userSize = userFeatures.count.toInt
    val splitPercent = Array.fill(userSize / SMALL_SIZE)(SMALL_SIZE) :+ (userSize - userSize / SMALL_SIZE * SMALL_SIZE)
    val userFeaturesSplit = userFeatures.randomSplit(splitPercent.map(_.toDouble))
    // 5. 重新构建子模型
    val models = userFeaturesSplit.map(userFe => new MatrixFactorizationModel(model.rank , userFe , productFeatures))
    // 6. 并保存模型
    models.zip(0 until(models.length)).foreach(currModel => currModel._1.save(sc,modelTable+"/m"+currModel._2))
    // 7. 关闭SparkContext
    sc.stop()
  }
  /**
    * 获得最新模型参数
    */
  def getModelArgs(sqlContext: HiveContext, url: String, table: String, user: String, password: String) = {
    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", password)
    properties.setProperty("driver", "com.mysql.jdbc.Driver")
    // ts（时间）   bestAlphas（权重） bestImplicitPref（模糊评分） bestIter(循环次数)  bestRank（值） bestReg（正则）
    val args = sqlContext.read.jdbc(url, table, properties) // should include "ts" and other args
    args.show()
    val ts_ranks_iterations_regs_alphas_implicitPrefs = args.orderBy(desc(ts)).take(1).apply(0)
    ALSParam(ts_ranks_iterations_regs_alphas_implicitPrefs.getDouble(1),
      ts_ranks_iterations_regs_alphas_implicitPrefs.getBoolean(2),
      ts_ranks_iterations_regs_alphas_implicitPrefs.getInt(3),
      ts_ranks_iterations_regs_alphas_implicitPrefs.getInt(4),
      ts_ranks_iterations_regs_alphas_implicitPrefs.getDouble(5))
  }
  /**
    * 处理参数
    */
  def handle_args(args: Array[String]) = {
    val inputTable = args(0) // 输入建模数据
    val useridCol = args(1) // 输入用户编码列名
    val urlidCol = args(2) // 输入网页编码列名
    val ratingCol = args(3) // 输入网页评分列名
    val url = args(4) // MySQL url连接
    val table = args(5) // MySQL 建模参数存储表
    val user = args(6) // MySQL 连接用户名
    val password = args(7) // MySQL 连接密码
    val modelTable = args(8) // 推荐结果存储表
    val appName = args(9) // 任务名
    (inputTable, useridCol, urlidCol, ratingCol,
      url, table, user, password, modelTable, appName)
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
      .append("<url>").append(" ")
      .append("<table>").append(" ")
      .append("<user>").append(" ")
      .append("<password>").append(" ")
      .append("<modelTable>").append(" ")
      .append("<appName>").append(" ")
    println(buff.toString())
  }
  case class ALSParam(bestAlphas: Double, bestImplicitPref: Boolean, bestIter: Int, bestRank: Int, bestReg: Double)
}