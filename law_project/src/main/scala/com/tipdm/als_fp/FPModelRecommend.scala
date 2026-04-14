package com.tipdm.als_fp

import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * FP 模型的关联规则的推荐结果
  * //@Author: fansy 
  * //@Time: 2018/10/29 10:51
  * //@Email: fansy1990@foxmail.com
  */
object FPModelRecommend {

  val URL_SETS_COLUMN = "URL_SETS_COLUMN"

  def generateRules(sc: SparkContext, sqlContext: SQLContext, trainTable: String,
                    uidCol: String, pidCol: String,
                    minSupport: Double, minConfidence: Double) = {
    //  1. 过滤只访问过一个url的用户数据，并把所有用户访问过的url进行集合（set）
    val data = sqlContext.sql("select " + uidCol + " , " + pidCol + " from " + trainTable)
      .distinct.groupBy(uidCol).agg(collect_set(col(pidCol)) as URL_SETS_COLUMN).filter(size(col(URL_SETS_COLUMN)) > 1)

    // 2. 把数据构建成 FPModel需要的数据
    val train = data.select(URL_SETS_COLUMN).rdd.map(row => row.getSeq[Long](0)).map(x => x.map(_.toInt).toArray)
    train.cache

    //  3.
    val fpg = new FPGrowth().setMinSupport(minSupport)
    val model = fpg.run(train)
    //  model.freqItemsets.count
    println("freqItemSet count : " + model.freqItemsets.count())
    val rules = model.generateAssociationRules(minConfidence)
    train.unpersist()
    rules
  }

  /**
    * 先按 antecedent.length降序，后按 confidence降序排列
    */
  val length_confidence = new Ordering[AssociationRules.Rule[Int]] {
    override def compare(x: Rule[Int], y: Rule[Int]): Int = if (x.antecedent.length != y.antecedent.length) {
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

  /**
    * 推荐
    * 针对每个train中评价过的数据，按照评价的个数分别从 rules_ordered 的对应位置（解析见下）进行往下遍历，得到recNum个值（这些值不能在train中出现，排除用户已经评价过的数据）后返回；
    * 对应位置： 如果当前rules_ordered里面的antecedent的最长为4，那么当validate中的数据长度大于或等于4，则从头开始遍历，如果等于3，那么则从rules_ordered的 rule.antecedent.length为3的地方开始匹配；
    *
    * @param train_visited
    * @param rules : 和 validate_visited 中url个数匹配的规则
    * @param recNum
    * @return
    */
  def getRecommend(train_visited: Set[Int], rules: Array[MyRule], recNum: Int): Array[Rating] = {
    val buff = new ArrayBuffer[Rating]()
    for (rule <- rules) {
      if (rule.antecedent.&(train_visited).size == rule.antecedent.size) { //  前缀全匹配
        if (!train_visited.contains(rule.consequent)) { // 推荐值不应出现在 train_visited中
          buff.append(Rating(-1, rule.consequent, rule.confidence))
        }
        if (buff.length >= recNum) {
          return buff.toArray
        }
      }
    }
    buff.toArray
  }

  case class MyRule(antecedent: Set[Int], consequent: Int, confidence: Double)

  /**
    * 推荐规则：
    *
    * 1. 把rule 按照 rule.antecedent.length 以及 rule.confidence进行排序 得到新的rules_ordered；
    * 2. validate leftOuterJoin train数据集，得到用户验证集中评价过的数据以及 训练集中已经评价过的数据， 命名为validate_train
    * 3. 针对每个train中评价过的数据，按照评价的个数分别从 rules_ordered 的对应位置（解析见下）进行往下遍历，得到recNum个值（这些值不能在train中出现，排除用户已经评价过的数据）后返回；
    *
    * 对应位置： 如果当前rules_ordered里面的antecedent的最长为4，那么当validate中的数据长度大于或等于4，则从头开始遍历，如果等于3，那么则从rules_ordered的 rule.antecedent.length为3的地方开始匹配；
    *
    * @param rules_origin
    * @param train
    * @param validate
    * @param recNum
    * @return
    */
  def getWithRecommend(rules_origin: RDD[AssociationRules.Rule[Int]], train: RDD[(Int, Set[Int])], validate: RDD[(Int, Set[Int])], recNum: Int)
  : RDD[(Int, (Array[Int], Array[Rating]))] = {
    // 1. 规则排序
    val antecedent_length_count = rules_origin.map(x => (x.antecedent.length, 1)).reduceByKey((x, y) => x + y).collect().sortBy(x => -x._1)
    val rules_ordered = rules_origin.collect().sorted(length_confidence)
      .map((x: AssociationRules.Rule[Int]) => MyRule(x.antecedent.toSet, x.consequent.head.toInt, x.confidence)) // 排过序的规则；
    val antecedent_count_sum = (0 to antecedent_length_count.length).map(x => antecedent_length_count.map(_._2).slice(0, x).sum)
    // 2. 整合validate和train数据
    val all_rules_ordered = antecedent_count_sum.map(x => rules_ordered.slice(x, rules_ordered.length))
    validate.join(train)
      .map { x =>
        val url_size = if (x._2._1.size >= antecedent_length_count.head._1) antecedent_length_count.head._1 else x._2._1.size
        (x._1, (x._2._1.toArray, getRecommend(x._2._2, all_rules_ordered(antecedent_length_count.length - url_size), recNum)))
      }
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
  def evalute(predictResult: RDD[(Int, (Array[Int], Array[Rating]))]): (Double, Double, Double, Int, Double, Double, Double) = {
    val tp_fp_fn = predictResult.map { x =>
      val realItems = x._2._1.toSet
      val recItems = x._2._2.map(_.product).toSet
      (realItems.&(recItems).size, recItems.diff(realItems).size, realItems.diff(recItems).size)
    }

    val precision_recall_fMeasure = tp_fp_fn.map[(Double, Double, Double)] { x =>
      val precision = if (x._1 == 0) 0.0 else x._1.toDouble / (x._1 + x._2)
      val recall = if (x._1 == 0) 0.0 else x._1.toDouble / (x._1 + x._3)
      (precision, recall, if (precision == 0.0 && recall == 0.0) 0.0 else 2 * precision * recall / (precision + recall))
    }.map(x => (1, x)).reduce((x1, x2) => (x1._1 + x2._1, (x1._2._1 + x2._2._1, x1._2._2 + x2._2._2, x1._2._3 + x2._2._3)))
    val precision = precision_recall_fMeasure._2._1 / precision_recall_fMeasure._1
    val recall = precision_recall_fMeasure._2._2 / precision_recall_fMeasure._1
    val fMeasure = precision_recall_fMeasure._2._3 / precision_recall_fMeasure._1

    (precision, recall, fMeasure, precision_recall_fMeasure._1, precision_recall_fMeasure._2._1, precision_recall_fMeasure._2._2, precision_recall_fMeasure._2._3)
  }


  def printUsage() = {
    val buff = new StringBuilder
    buff.append("Usage: com.tipdm.als_fp.FPModelModifyRecommend ").append(" ")
      .append(" <trainTable> ")
      .append(" <validateTable> ")
      .append(" <uidCol> ")
      .append(" <pidCol> ")
      .append(" <minSupport> ")
      .append(" <minConfidence> ")
      .append(" <recNum> ")
      .append(" <appName> ")
  }

  def handle_args(args: Array[String]) = {
    val trainTable = args(0)
    val validateTable = args(1)
    val uidCol = args(2)
    val pidCol = args(3)
    val minSupport = args(4).toDouble
    val minConfidence = args(5).toDouble
    val recNum = args(6).toInt
    val appName = args(7)
    (trainTable, validateTable, uidCol, pidCol, minSupport, minConfidence, recNum, appName)
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 8) {
      printUsage()
      System.exit(1)
    }
    // 0. 参数处理
    val (trainTable, validateTable, uidCol, pidCol, minSupport, minConfidence, recNum, appName) = handle_args(args)

    // 1. 初始化
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    // 2. 生成规则
    val rules = generateRules(sc, sqlContext, trainTable, uidCol, pidCol, minSupport, minConfidence)

    // 3. 推荐
    val train = sqlContext.sql("select * from " + trainTable).select(uidCol, pidCol).groupBy(uidCol).agg(collect_set(pidCol)).rdd.repartition(24).map(row => (row.getLong(0).toInt, (row.getSeq[Long](1)).map(_.toInt).toSet))
    val validate = sqlContext.sql("select * from " + validateTable).select(uidCol, pidCol).groupBy(uidCol).agg(collect_set(pidCol)).rdd.repartition(8).map(row => (row.getLong(0).toInt, (row.getSeq[Long](1)).map(_.toInt).toSet))
    val recommended = getWithRecommend(rules, train, validate, recNum)

    // 4. 评估
    val (p, r, f, size, p_sum, r_sum, f_sum) = evalute(recommended)
    println("precision :" + p + " \t recall: " + r + " \t F1:" + f + " \t size :" + size)

    sc.stop()

  }
}
