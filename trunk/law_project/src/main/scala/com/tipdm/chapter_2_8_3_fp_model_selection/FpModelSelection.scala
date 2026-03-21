package com.tipdm.chapter_2_8_3_fp_model_selection

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer

/**
  * 根据模型参数范围，寻找最优模型参数
  * //@Author: fansy 
  * //@Time: 2019/1/15 15:00
  * //@Email: fansy1990@foxmail.com
  */
object FpModelSelection {
  val uidCol = "userid"
  val urlCol = "fullurl"
  val time_url = "time_url"
  val time_url_list = "time_url_list"
  val timeCol = "timestamp_format"
  val interval = 7
  val recNum = 1
  // 先按 antecedent.length降序，后按 confidence降序排列
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

  def main(args: Array[String]): Unit = {
    if (args.length != 10) {
      printUsage()
      System.exit(1)
    }
    //  0. 参数处理
    val (trainTable, validateTable, transactionColName, minSupports, minConfidences, url, table, user, password, appName) = handle_args(args)

    //  1. 初始化
    val conf = new SparkConf().setAppName(appName)//.setExecutorEnv("SPARK_JAVA_OPTS","-Xss4096k")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    // 2. 加载数据
    val trainData = sqlContext.read.table(trainTable).select(transactionColName)
      .rdd.map(row => row.getSeq[String](0)).map(x => x.toArray)
    trainData.cache()
    val validateData = generateInputOutput(sqlContext.read.table(validateTable), interval)

    // 3. 寻找最优参数
    var bestMinSupport = minSupports.head
    var bestMinConfidence = minConfidences.head
    var minFScore = Double.MaxValue
    for (minSupport <- minSupports; minConfidence <- minConfidences) {
      val fpg = new FPGrowth().setMinSupport(minSupport).setNumPartitions(4)
      val model = fpg.run(trainData)
      val (fScore, _, _) = evaluate(getWithRecommendWithInOut(model.generateAssociationRules(minConfidence),
        validateData.rdd.map(row => (row.getSeq[String](1), row.getSeq[String](2))), recNum))

      if (fScore < minFScore) {
        bestMinSupport = minSupport
        bestMinConfidence = minConfidence
        minFScore = fScore
      }
    }

    // 4. 存储模型最优参数
    save_model_args(sc, sqlContext, bestMinSupport, bestMinConfidence, minFScore, url, table, user, password)

    // 4. 关闭SparkContext
    sc.stop()

  }

  /**
    * 保存模型最优参数
    *
    * @param sc
    * @param sqlContext
    * @param bestMinSupport
    * @param bestMinConfidence
    * @param minFScore
    * @param url
    * @param table
    * @param user
    * @param password
    * @return
    */
  def save_model_args(sc: SparkContext, sqlContext: HiveContext, bestMinSupport: Double, bestMinConfidence: Double,
                      minFScore: Double, url: String, table: String, user: String, password: String) = {
    val properties = new Properties()
    properties.setProperty("user", user)
    properties.setProperty("password", password)
    import sqlContext.implicits._
    sc.parallelize(Seq(MinSupport_Confidence(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()), bestMinSupport, bestMinConfidence, minFScore))).toDF
      .write.mode(SaveMode.Append).jdbc(url, table, properties)
  }

  /**
    * 处理验证集
    *
    * @param data
    * @param interval
    * @return
    */
  def generateInputOutput(data: DataFrame, interval: Int): DataFrame = {
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val split_in_out = udf { (time_url_arr: Seq[org.apache.spark.sql.Row], interval: Int) =>
      val time_url_list = time_url_arr.map(x => (x.getString(0), x.getString(1))).sortBy(x => x._1)
      val final_interval = interval * 60 * 1000
      val first = time_url_list.head // first._2 is the input
    val second = time_url_list.tail.takeWhile(p =>
      (simpleDateFormat.parse(p._1).getTime - simpleDateFormat.parse(first._1).getTime) <= final_interval)
      .map(p => p._2)
      Array(Seq(first._2), second)
    }
    data.select(col(uidCol), struct(col(timeCol), col(urlCol)) as time_url)
      .groupBy(uidCol).agg(collect_list(time_url) as time_url_list).withColumn(time_url_list, split_in_out(col(time_url_list), lit(interval))).select(col(uidCol), col(time_url_list).getItem(0).as("urls_in"), col(time_url_list).getItem(1).as("urls_out"))
  }

  /**
    * 推荐
    *
    * @param rules_origin
    * @param validate_for_test
    * @param recNum
    * @return
    */
  def getWithRecommendWithInOut(rules_origin: RDD[AssociationRules.Rule[String]], validate_for_test: RDD[(Seq[String], Seq[String])], recNum: Int)
  : RDD[(Seq[String], Seq[String], Array[(String, Double)])] = {
    // 1. 规则排序
    val rules_ordered = rules_origin.collect().sorted(length_confidence) // 排过序的规则；
    validate_for_test.map(visited => getRecommend(visited, rules_ordered, recNum))
  }

  // 推荐函数
  def getRecommend(visited: (Seq[String], Seq[String]), rules_ordered: Array[Rule[String]], recNum: Int): (Seq[String],
    Seq[String], Array[(String, Double)]) = {
    val buff = new ArrayBuffer[(String, Double)]()
    val visited_set = visited
    for (rule <- rules_ordered) {
      if (visited_set._1.size >= rule.antecedent.length) {
        // 不是对应位置，则跳过
        if (visited_set._1.toSet.&(rule.antecedent.toSet).size == rule.antecedent.size) {
          // 最佳匹配
          for (recItem <- rule.consequent) {
            buff.append((recItem, rule.confidence))
            if (buff.length >= recNum) {
              return (visited._1, visited._2, buff.toArray)
            }
          }
        }
      }
    }
    (visited._1, visited._2, buff.toArray)
  }

  /**
    * 定义评估函数
    *
    * @param predictResult
    * @return
    */
  def evaluate(predictResult: RDD[(Seq[String], Seq[String], Array[(String, Double)])]): (Double, Double, Double) = {
    val tp_fp_fn = predictResult.map { x =>
      val realItems = x._2.toSet
      val recItems = x._3.map(_._1).toSet
      val tp = (realItems.&(recItems)).size
      val fp = recItems.diff(realItems).size
      val fn = realItems.diff(recItems).size
      (tp.toDouble, fp.toDouble, fn.toDouble)
    }
    val precision_recall_fMeasure = tp_fp_fn.map[(Double, Double, Double)] { x =>
      val precision = if (x._1 == 0.0 && x._2 == 0.0) 0.0 else x._1 / (x._1 + x._2)
      val recall = if (x._1 == 0.0 && x._3 == 0.0) 0.0 else x._1 / (x._1 + x._3)
      (
        precision, recall,
        if (precision == 0.0 && recall == 0.0) 0.0 else 2 * precision * recall / (precision + recall)
      )
    }.map(x => (1, x)).reduce((x1, x2) =>
      (x1._1 + x2._1, (x1._2._1 + x2._2._1, x1._2._2 + x2._2._2, x1._2._3 + x2._2._3)))
    val precision = precision_recall_fMeasure._2._1 / precision_recall_fMeasure._1
    val recall = precision_recall_fMeasure._2._2 / precision_recall_fMeasure._1
    val fMeasure = precision_recall_fMeasure._2._3 / precision_recall_fMeasure._1

    //    (precision, recall, fMeasure, precision_recall_fMeasure._1, precision_recall_fMeasure._2._1, precision_recall_fMeasure._2._2, precision_recall_fMeasure._2._3)
    (fMeasure, precision, recall)
  }

  /**
    * 处理参数
    *
    * @param args 输入参数
    * @return
    */
  def handle_args(args: Array[String]) = {
    val trainTable = args(0) // 输入训练集数据
    val validateTable = args(1) // 输入验证集数据
    val transactionColName = args(2) // 输入数据列名
    val minSupports = args(3).split(",").map(_.trim.toDouble)
    val minConfidence = args(4).split(",").map(_.trim.toDouble)
    val url = args(5) // MySQL url连接
    val table = args(6) // MySQL 建模参数存储表
    val user = args(7) // MySQL 连接用户名
    val password = args(8) // MySQL 连接密码
    val appName = args(9) // 任务名
    (trainTable, validateTable, transactionColName, minSupports, minConfidence, url, table, user, password, appName)
  }

  /**
    * 使用说明
    */
  def printUsage(): Unit = {
    val buff = new StringBuilder
    buff.append("Usage : com.tipdm.chapter_2_8_3_fp_model_selection.FpModelSelection").append(" ")
      .append("<trainTable>").append(" ")
      .append("<validateTable>").append(" ")
      .append("<transactionColName>").append(" ")
      .append("<minSupport1,minSupport2,...>").append(" ")
      .append("<minConfidence1,minConfidence2,...>").append(" ")
      .append("<url>").append(" ")
      .append("<table>").append(" ")
      .append("<user>").append(" ")
      .append("<password>").append(" ")
      .append("<appName>").append(" ")
    println(buff.toString())
  }

  case class MinSupport_Confidence(ts: String, minSupport: Double, minConfidence: Double, minFScore: Double)

}
