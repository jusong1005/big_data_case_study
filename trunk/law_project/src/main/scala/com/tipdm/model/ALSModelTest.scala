package com.tipdm.model

import com.tipdm.util.CommonUtil._
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object ALSModelTest {

  case class ALSParam(rank: Int, iteration: Int, reg: Double, alpha: Double, implicitPrefs: Boolean)

  /** suling modify at 2018-11-15
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

  def getModelParameter(parameterData: DataFrame, bestrank: String, bestiter: String, bestalpha: String,
                        bestreg: String, bestimplicitprefs: String) = {
    val parameter = parameterData.select(bestrank, bestiter, bestalpha, bestreg, bestimplicitprefs).first()
    val rank = parameter.getInt(0)
    val iter = parameter.getInt(1)
    val alpha = parameter.getDouble(2)
    val reg = parameter.getDouble(3)
    val implicitPref = parameter.getBoolean(4)
    (rank, iter, alpha, reg, implicitPref)
  }

  def parseArgs(args: Array[String]) = {
    (args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7))
  }

  def main(args: Array[String]): Unit = {
    val (inputTable, parameterTable, modelPath, bestrank, bestiter, bestalpha, bestreg, bestimplicitprefs) = parseArgs(args)
    // 初始化
    val sc = new SparkContext(new SparkConf().setAppName("ALS model"))
    val sqlContext = new HiveContext(sc)
    // 读取数据
    val data = sqlContext.sql("select * from " + inputTable).rdd.map(x => Rating(x.getLong(0).toInt, x.getLong(1).toInt, x(2).toString.toDouble))
    val parameterData = sqlContext.sql("select * from " + parameterTable)
    val (rank, iter, alpha, reg, implicitPref) = getModelParameter(parameterData, bestrank, bestiter, bestalpha, bestreg, bestimplicitprefs)
    val model = getAls(data, ALSParam(rank, iter, reg, alpha, implicitPref))
    deletePath(modelPath, sqlContext)
    model.save(sc, modelPath)
  }
}
