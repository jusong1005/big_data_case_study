package com.tipdm.chapter_2_7_2_als_model_selection

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.AlsUtil._
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ALS 模型选择
  */
object AlsModelSelection {
  /**
    * 映射URL访问次数到评分
    * 规则：
    * 1~8  ：1~8分
    * 9~23 ： 9分
    * > 23 :  10分
    *
    * @param times
    * @return
    */
  def trans_times_2_rate(times: Long): Double = {
    if (times <= 8) {
      times
    } else if (times > 23) {
      10
    } else {
      9
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 16) {
      printUsage()
      System.exit(1)
    }
    //  0. 参数处理
    val (trainTable, validateTable, ranks, iterations, regs, alphas, implicitPrefs, userCol, itemCol, ratingCol, url, table, user, password, recNums, appName) = handle_args(args)

    //  1. 初始化
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    //  2. 读取数据
    val train = sqlContext.sql("select * from " + trainTable)
    val validate = sqlContext.sql("select * from " + validateTable)

    //  3.寻找最优参数
    var bestRank = ranks.head
    var bestIter = iterations.head
    var bestReg = regs.head
    var bestAlpha = alphas.head
    var bestImplicitPrefs = implicitPrefs.head
    var bestF = 0.0
    //    var bestModel: MatrixFactorizationModel = null
    //  转换为Rating类型数据
    val trainRdd = train.rdd.map(x => Rating(x.getInt(0), x.getInt(1), x.getDouble(2)))
      .repartition(24) //  TODO this block is ok？
    //  转换为RDD
    val validateRdd = validate.select(userCol, itemCol).distinct().rdd.map(x => (x.getInt(0), x.getInt(1)))// the rate is useless
      .repartition(8) //  TODO this block is ok？
    trainRdd.cache()
    validateRdd.cache()
    var i = 0
    for (recNum <- recNums) {
      for (rank <- ranks; iter <- iterations; reg <- regs; alpha <- alphas; implicitPref <- implicitPrefs) {
        //  建模
        val model = getAls(trainRdd, ALSParam(rank, iter, reg, alpha, implicitPref))
        //  预测
        val predictResult = getWithRecommend(model, validateRdd, 10, rank)
        //  评估 ,Evaluate后的数据进行一次partition操作
        val precision_recall_fMeasure = evalute(predictResult).map(x => (1, x)).reduce((x1, x2) =>
          (x1._1 + x2._1, (x1._2._1 + x2._2._1, x1._2._2 + x2._2._2, x1._2._3 + x2._2._3)))
        val precision = precision_recall_fMeasure._2._1 / precision_recall_fMeasure._1
        val recall = precision_recall_fMeasure._2._2 / precision_recall_fMeasure._1
        val fMeasure = precision_recall_fMeasure._2._3 / precision_recall_fMeasure._1
        if (fMeasure > bestF) {
          bestF = if (fMeasure.equals(Double.NaN)) -1.0 else fMeasure
          bestAlpha = alpha
          bestImplicitPrefs = implicitPref
          bestIter = iter
          bestRank = rank
          bestReg = reg
        }
        i = i + 1
        println(new java.util.Date()+ ": 当前循环次数: "+i)

      }
      //  3. 保存最优参数到mysql
      save_model_args(sc, sqlContext, bestAlpha, bestImplicitPrefs, bestIter, bestRank, bestReg, bestF, url, table, user, password)
    }
    //  4. 关闭sc
    sc.stop()
  }

  /**
    * 使用说明
    */
  def printUsage() = {
    val buff = new StringBuilder

    buff.append("Usage : com.tipdm.chapter_2_7_2_asl_model_selection.AlsModelSelection").append(" ")
      .append("<trainTable>").append(" ")
      .append("<validateTable>").append(" ")
      .append("<rank1,rank2,...>").append(" ")
      .append("<iter1,iter2,...>").append(" ")
      .append("<reg1,reg2,...>").append(" ")
      .append("<alpha1,alpha2,...>").append(" ")
      .append("<implicitPref1,implicitPref2>").append(" ")
      .append("<userCol>").append(" ")
      .append("<itemCol>").append(" ")
      .append("<ratingCol>").append(" ")
      .append("<bestModelPath>").append(" ")
      .append("<recNum1,recNum2,...>").append(" ")
      .append("<appName>").append(" ")
    println(buff.toString())
  }

  /**
    * 处理参数
    *
    * @param args 输入参数
    * @return
    */
  def handle_args(args: Array[String]) = {
    val trainTable = args(0) //训练集
    val validateTable = args(1) //验证集
    val ranks = args(2).split(",").map(_.trim.toInt) //秩
    val iterations = args(3).split(",").map(_.trim.toInt) //迭代次数
    val regs = args(4).split(",").map(_.trim.toDouble) //lambda值
    val alphas = args(5).split(",").map(_.trim.toDouble) //alpha值
    val implicitPrefs = args(6).split(",").map(_.trim.toBoolean) //是否调用ALS.trainImplicit构建模型
    val userCol = args(7) //用户ID列
    val itemCol = args(8) //网页ID列
    val ratingCol = args(9) //用户评分
    val url = args(10) // MySQL url连接
    val table = args(11) // MySQL 建模参数存储表
    val user = args(12) // MySQL 连接用户名
    val password = args(13) // MySQL 连接密码
    val recNums = args(14).split(",").map(_.trim.toInt) //推荐个数
    val appName = args(15) //appName
    (trainTable, validateTable, ranks, iterations, regs, alphas, implicitPrefs, userCol, itemCol, ratingCol, url, table, user, password, recNums, appName)
  }

  /**
    * 保存模型最优参数
    *
    * @param sc
    * @param sqlContext
    * @param bestAlpha
    * @param bestImplicitPrefs
    * @param bestIter
    * @param bestRank
    * @param bestReg
    * @param bestF
    * @param url
    * @param table
    * @param user
    * @param password
    * @return
    */
  def save_model_args(sc: SparkContext, sqlContext: HiveContext, bestAlpha: Double, bestImplicitPrefs: Boolean,
                      bestIter: Double, bestRank: Double, bestReg: Double, bestF: Double, url: String, table: String, user: String, password: String) = {
    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", password)
    import sqlContext.implicits._
    sc.parallelize(Seq(Optimal_Parameter(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()), bestAlpha, bestImplicitPrefs, bestIter, bestRank, bestReg, bestF))).toDF
      .write.mode(SaveMode.Append).jdbc(url, table, properties)
  }
  /**
    * 获取ALS模型
    *
    * @param train
    * @param alsParam
    * @return
    */
  def getAls(train: RDD[Rating], alsParam: ALSParam) = {
    if (alsParam.implicitPrefs) {
      ALS.trainImplicit(train, alsParam.rank, alsParam.iteration, alsParam.reg, alsParam.alpha)
    } else {
      ALS.train(train, alsParam.rank, alsParam.iteration, alsParam.reg)
    }
  }

  /**
    * 返回实际看过的和预测的集合
    * 不要直接使用所有用户进行推荐，效率太低
    *
    * @param model
    * @param validate
    * @param recNum
    * @return
    */
  def getWithRecommend(model: MatrixFactorizationModel, validate: RDD[(Int, Int)], recNum: Int, rank: Int)
  : RDD[(Int, (Array[Int], Array[Rating]))] = {
    val validate_users = validate.map(x => (x._1, 0)).distinct()
    // 需要distinct //取出在训练集中出现的用户
    val userFeatures = model.userFeatures.join(validate_users).map(x => (x._1, x._2._1)) //
    val productFeatures = model.productFeatures
    val user_recomend = recommendForAll(rank, userFeatures, productFeatures, recNum).map {
      case (user, top) =>
        val ratings = top.map { case (product, rating) => Rating(user, product, rating) }
        (user, ratings)
    }
    //  存在的用户对推荐的模型才有意义，所以求交集，而非左连接
    validate.combineByKey[Array[Int]](
      (x: Int) => Array(x),
      (c: Array[Int], v: Int) => c :+ v,
      (c1: Array[Int], c2: Array[Int]) => c1 ++ c2
    ).join(user_recomend)
  }

  /**
    * 评估函数: 由于有多个用户，所以所有用户求平均值即可；
    *
    * TP : true positives （tp）: 推荐的用户看了
    * FP : false positives（fp）：推荐了用户没看
    * FN : false negatives（fn）：没推荐，用户看了
    * Precision=TP/(TP+FP)
    * Recall=TP/(TP+FN)
    * F1 = 2P*R/(P + R)
    *
    * @param predictResult
    * @return
    */
  def evalute(predictResult: RDD[(Int, (Array[Int], Array[Rating]))]): RDD[(Double, Double, Double)] = {
    val tp_fp_fn = predictResult.map { x =>
      val realItems = x._2._1.toSet
      val recItems = x._2._2.map(_.product).toSet
      (realItems.&(recItems).size, recItems.diff(realItems).size, realItems.diff(recItems).size)
    }
    tp_fp_fn.map[(Double, Double, Double)] { x =>
      val precision = if (x._1 == 0) 0.0 else x._1.toDouble / (x._1 + x._2)
      val recall = if (x._1 == 0) 0.0 else x._1.toDouble / (x._1 + x._3)
      (precision, recall, if (precision == 0.0 && recall == 0.0) 0.0 else 2 * precision * recall / (precision + recall))
    }
  }

  case class ALSParam(rank: Int, iteration: Int, reg: Double, alpha: Double, implicitPrefs: Boolean)

  case class Optimal_Parameter(ts: String, bestAlpha: Double, bestImplicitPrefs: Boolean, bestIter: Double, bestRank: Double, bestReg: Double, bestF: Double)

}
