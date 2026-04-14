package com.tipdm.model

import com.tipdm.util.CommonUtil._
import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, collect_set, size}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType, _}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * suling modify at 2018-11-16
  */
object FPModelTestV1 {
  /**
    * 训练FP模型
    * // * @param table law_init1.data_101003_url_gt_one
    *
    * @return
    */
  val URL_SETS_COLUMN = "URL_SETS_COLUMN"

  /**
    * 构建FP模型，并且根据提供的支持度和置信度筛选规则
    *
    * @param sc
    * @param sqlContext
    * @param trainTable
    * @param uidCol
    * @param pidCol
    * @param minSupport
    * @param minConfidence
    * @return
    */
  def generateRules(sc: SparkContext, sqlContext: HiveContext, trainTable: String,
                    uidCol: String, pidCol: String,
                    minSupport: Double, minConfidence: Double) = {
    // 1. 过滤只访问过一个url的用户数据，并把所有用户访问过的url进行集合（set）
    val data = sqlContext.sql("select " + uidCol + " , " + pidCol + " from " + trainTable).distinct.groupBy(uidCol).agg(collect_set(col(pidCol)) as URL_SETS_COLUMN).filter(size(col(URL_SETS_COLUMN)) > 1)

    // 2. 把数据构建成 FPModel需要的数据
    val train = data.select(URL_SETS_COLUMN).rdd.map(row => row.getSeq[String](0)).map(x => x.map(_.trim).toArray)
    train.cache

    // 3.建模
    val fpg = new FPGrowth().setMinSupport(minSupport)
    val model = fpg.run(train)
    val rules = model.generateAssociationRules(minConfidence)
    train.unpersist()
    rules
  }

  /** 将规则先按照规则前半部分长度降序排序，对于前半部分长度相同的规则，按照置信度降序排序
    * 先按 antecedent.length降序，后按 confidence降序排列
    */
  val length_confidence = new Ordering[AssociationRules.Rule[String]] {
    override def compare(x: Rule[String], y: Rule[String]): Int = if (x.antecedent.length != y.antecedent.length) {
      y.antecedent.length - x.antecedent.length
    } else {
      if (-x.confidence + y.confidence > 0) {
        1
      } else if (x.confidence == y.confidence) {
        0
      } else {
        -1
      }
    }
  }

  def parseArgs(args: Array[String]) = {
    // 输入表，最优参数输入表,uid,pid,规则表
    (args(0), args(1), args(2), args(3), args(4))
  }

  def main(args: Array[String]): Unit = {
    val (inputTable, parameterTable, uidCol, pidCol, outTable) = parseArgs(args)
    // 初始化
    val sc = new SparkContext(new SparkConf().setAppName("FP model V1").setExecutorEnv("SPARK_JAVA_OPTS", "-Xss4096k"))
    val sqlContext = new HiveContext(sc)
    // 读取模型
    val parameterData = sqlContext.sql("select * from " + parameterTable)
    val minSupport = parameterData.select("minsupports").first.getDouble(0)
    val minConfidence = parameterData.select("minconfidences").first.getDouble(0)
    val rules = generateRules(sc, sqlContext, inputTable, uidCol, pidCol, minSupport, minConfidence)
    val schema = StructType(List(
      StructField("antecedent", ArrayType(StringType), nullable = true),
      StructField("consequent", ArrayType(StringType), nullable = true),
      StructField("confidences", DoubleType, nullable = true)
    ))
    val result = rules.filter(x => x.antecedent.length > 0 && x.consequent.length > 0).collect.sorted(length_confidence)
    val result2 = sc.parallelize(result).map(x => Row(x.antecedent, x.consequent, x.confidence))
    val result3 = sqlContext.createDataFrame(result2, schema)
    saveHiveTable(sqlContext, result3, outTable, true)
  }
}