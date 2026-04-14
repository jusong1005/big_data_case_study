package com.tipdm.optimization

import com.tipdm.als_fp.AlsModelSelection._
import com.tipdm.util.CommonUtil._
import org.apache.spark.AlsUtil.recommendForAll
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, lit, udf}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * suling
  * 该类的作用主要是使用输入的数据跟参数建模、推荐，并采用评估方法得到最优模型，将模型参数保存
  *
  */
object ALSOptimization {
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
    * @param model    模型
    * @param validate 包含两个字段（用户ID，网页id）
    * @param recNum   推荐个数
    * @param rank     属性的秩
    * @return
    */
  def getWithRecommend(model: MatrixFactorizationModel, validate: RDD[(Int, Int)], recNum: Int, rank: Int)
  : RDD[(Int, (Array[Int], Array[Rating]))] = {
    val validate_users = validate.map(x => (x._1, 0)).distinct() //取出用户ID并去重
    val userFeatures = model.userFeatures.join(validate_users).map(x => (x._1, x._2._1)) //找出在训练集中出现过的用户以及模型为这个用户计算出的属性，userFeature类型为RDD（userID，Array【double】），元组中的内容是用户ID以及模型为用户计算出的属性
    val productFeatures = model.productFeatures //(pid,Array[Double]),pid以及为这个产品计算的features
    val user_recomend = recommendForAll(rank, userFeatures, productFeatures, recNum).map { //这个方法可以得到为这个用户推荐的产品以及，每个产品的评分
      case (user, top) =>
        val ratings = top.map { case (product, rating) => Rating(user, product, rating) }
        (user, ratings)
    }

    // 存在的用户对推荐的模型才有意义，所以求交集，而非左连接
    validate.combineByKey[Array[Int]]( //将每个用户访问过的产品组合成数组，跟推荐的结果连接
      (x: Int) => Array(x),
      (c: Array[Int], v: Int) => c :+ v,
      (c1: Array[Int], c2: Array[Int]) => c1 ++ c2
    ).join(user_recomend)

  }

  /**
    * 评估: 由于有多个用户，所以所有用户求平均值即可；
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
      (realItems.&(recItems).size, recItems.diff(realItems).size, realItems.diff(recItems).size) //tp,fp,fn
    }
    tp_fp_fn.map[(Double, Double, Double)] { x =>
      val precision = if (x._1 == 0) 0.0 else x._1.toDouble / (x._1 + x._2)
      val recall = if (x._1 == 0) 0.0 else x._1.toDouble / (x._1 + x._3)
      (
        precision, recall,
        if (precision == 0.0 && recall == 0.0) 0.0 else 2 * precision * recall / (precision + recall)
      )
    }
  }

  def modelSelection(spark: SparkContext, train: DataFrame, validate: DataFrame, ranks: Array[Int],
                     iterations: Array[Int], regs: Array[Double], alphas: Array[Double],
                     implicitPrefs: Array[Boolean],
                     userCol: String, itemCol: String, ratingCol: String, recNums: Array[Int], sqlContext: HiveContext, outTable: String) = {
    var bestRank = 8
    var bestIter = 8
    var bestReg = 0.1
    var bestAlpha = 1.0
    var bestImplicitPrefs = true
    var bestF = 0.0
    var bestModel: MatrixFactorizationModel = null

    // 转换为RDD
    val times_2_rate = udf { (times: Long) => trans_times_2_rate(times) }
    val trainRdd = train.select(userCol, itemCol).groupBy(userCol, itemCol).agg(count(lit(1)) as ratingCol)
      .withColumn(ratingCol, times_2_rate(col(ratingCol)))
      .repartition(24) // TODO this block is ok？
      .rdd.map(x => Rating(x.getLong(0).toInt, x.getLong(1).toInt, x.getDouble(2)))
    val validateRdd = validate.select(userCol, itemCol).distinct().rdd.map(x => (x.getLong(0).toInt, x.getLong(1).toInt)) // the rate is useless
      .repartition(8) // TODO this block is ok？
    trainRdd.cache()
    validateRdd.cache()
    for (recNum <- recNums) {
      println("========================================recNum:" + recNum)
      println("rank \t iteration \t reg \t implicitPrefs \t alpha \t precision \t recall \t fMeasure ")
      for (rank <- ranks; iter <- iterations; reg <- regs; alpha <- alphas; implicitPref <- implicitPrefs) {
        // 1. 建模
        val model = getAls(trainRdd, ALSParam(rank, iter, reg, alpha, implicitPref))
        // 2. 预测
        val predictResult = getWithRecommend(model, validateRdd, 10, rank)
        // 3. 评估 ,Evaluate后的数据进行一次partition操作
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
          bestModel = model
        }
        println(rank + "\t" + iter + "\t" + reg + "\t" + implicitPref + "\t" + alpha + "\t" + precision + "\t" + recall + "\t" + fMeasure + "\t" + precision_recall_fMeasure._1 + ": " + precision_recall_fMeasure._2.toString())
      }
      println("best model parameters : ")
      println("rank: " + bestRank + "\t iteration: " + bestIter + "\t" + "reg: " + bestReg +
        "\t" + "implicitPrefs:" + bestImplicitPrefs + "\t" + "alpha:" + bestAlpha)
      import sqlContext.implicits._
      val bestArgs = spark.parallelize(List((bestRank, bestIter, bestAlpha, bestReg, bestImplicitPrefs, bestF))).repartition(1).toDF("bestRank", "bestIter", "bestAlpha", "bestReg", "bestImplicitPrefs", "bestF")
      saveHiveTable(sqlContext, bestArgs, outTable, true)
    }

    trainRdd.unpersist()
    validateRdd.unpersist()
  }

  /**
    * 映射URL访问次数到评分
    * 规则：
    * 1~8  ：1~8
    * 9~23 ： 9
    * > 23 :  10
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

  def printUsage() = {
    val buff = new StringBuilder
    buff.append("Usage : com.tipdm.als_fp.AlsModelSelection").append(" ")
      .append("<trainTable>").append(" ")
      .append("<validateTable>").append(" ")
      .append("<rank1,rank2,rank3>").append(" ")
      .append("<iter1,iter2,iter3>").append(" ")
      .append("<reg1,reg2,reg3>").append(" ")
      .append("<alpha1,alpha2,alpha3>").append(" ")
      .append("<implicitPref1,implicitPref2>").append(" ")
      .append("<userCol>").append(" ")
      .append("<itemCol>").append(" ")
      .append("<ratingCol>").append(" ")
      //.append("<bestModelPath>").append(" ")
      .append("<recNum1,recNum2>").append(" ")
      .append("<appName>").append(" ")
      .append("<outTable>").append(" ")
    println(buff.toString())
  }

  def handle_args(args: Array[String]) = {
    val trainTable = args(0)
    val validateTable = args(1)
    val ranks = args(2).split(",").map(_.trim.toInt)
    val iterations = args(3).split(",").map(_.trim.toInt)
    val regs = args(4).split(",").map(_.trim.toDouble)
    val alphas = args(5).split(",").map(_.trim.toDouble)
    val implicitPrefs = args(6).split(",").map(_.trim.toBoolean)
    val userCol = args(7)
    val itemCol = args(8)
    val ratingCol = args(9)
    //val bestModelPath = args(10)
    val recNums = args(10).split(",").map(_.trim.toInt)
    val appName = args(11)
    val outTable = args(12)
    (trainTable, validateTable, ranks, iterations, regs, alphas, implicitPrefs, userCol, itemCol, ratingCol, recNums, appName, outTable)
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 13) {
      printUsage()
      System.exit(1)
    }
    // 0. 参数处理
    val (trainTable, validateTable, ranks, iterations, regs, alphas, implicitPrefs, userCol, itemCol, ratingCol, recNums, appName, outTable) = handle_args(args)

    // 1. 初始化
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    // 2. 读取数据
    val train = sqlContext.sql("select * from " + trainTable)
    val validate = sqlContext.sql("select * from " + validateTable)

    // 3. 调用
    modelSelection(sc, train, validate, ranks, iterations, regs, alphas, implicitPrefs, userCol, itemCol, ratingCol, recNums, sqlContext, outTable)

    // 4. close
    sc.stop()
  }
}
