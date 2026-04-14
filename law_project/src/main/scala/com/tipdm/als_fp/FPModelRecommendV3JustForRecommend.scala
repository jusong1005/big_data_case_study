package com.tipdm.als_fp

import java.text.SimpleDateFormat

import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * 用于书籍中 数据探索 FP模块
  * //@Author: fansy 
  * //@Time: 2019/1/9 17:15
  * //@Email: fansy1990@foxmail.com
  */
object FPModelRecommendV3JustForRecommend {


  /**
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



  /**
    * 推荐
    * 针对每个train中评价过的数据，按照评价的个数分别从 rules_ordered 的对应位置（解析见下）进行往下遍历，得到recNum个值（这些值不能在train中出现，排除用户已经评价过的数据）后返回；
    * 对应位置： 如果当前rules_ordered里面的antecedent的最长为4，那么当visited中的数据长度大于或等于4，则从头开始遍历，如果等于3，那么则从rules_ordered的 rule.antecedent.length为3的地方开始匹配；
    * @param visited
    * @param rules_ordered
    * @param recNum
    * @return
    */
  def getRecommend(visited: Seq[String], rules_ordered: Array[Rule[String]], recNum: Int):(Seq[String], Array[(String, Double)])  = {
    val buff = new ArrayBuffer[(String, Double)]()
    val visited_set = visited.toSet
    for (rule <- rules_ordered) {
      if (visited_set.size >= rule.antecedent.length) {
        // 不是对应位置，则跳过
        if (visited_set.&(rule.antecedent.toSet).size == rule.antecedent.size) {
          // 最佳匹配
          for (recItem <- rule.consequent) {
            buff.append((recItem, rule.confidence))
            if (buff.length >= recNum) {
              return (visited, buff.toArray)
            }
          }
        }
      }
    }
    (visited,buff.toArray)
  }

  /**
    * 推荐规则：
    *
    * 1. 把rule 按照 rule.antecedent.length 以及 rule.confidence进行排序 得到新的rules_ordered；
    * 2. 针对每个用户访问过的数据，按照访问过的个数分别从 rules_ordered 的对应位置（解析见下）进行往下遍历，判断当前rule和访问过的数据是否达到“最佳匹配”，如果达到最佳匹配，则把推荐值加入推荐列表；
    * 3. 当遍历完rules_ordered或推荐列表达到recNum个值则返回；
    *
    * 对应位置： 如果当前rules_ordered里面的antecedent的最长为4，那么当validate中的数据长度大于或等于4，则从头开始遍历，如果等于3，那么则从rules_ordered的 rule.antecedent.length为3的地方开始匹配；
    *
    * 最佳匹配：
    * 1）	假设用户访问页面的集合为C_test，某条关联规则如下（C1 -> C2 : confidence）。
    * 2）	那么如果 size（C_test ∩ C1 ）== size（C1），那么此时C_test的最佳匹配就是C1，那么就可以使用C2来进行推荐。
    *
    * @param rules_origin
    * @param validate_for_test
    * @param recNum
    * @return
    */
  def getWithRecommend(rules_origin: RDD[AssociationRules.Rule[String]], validate_for_test: RDD[Seq[String]], recNum: Int)
  : RDD[(Seq[String], Array[(String, Double)])] = {
    // 1. 规则排序
    val antecedent_length_count = rules_origin.map(x => (x.antecedent.length, 1)).reduceByKey((x, y) => x + y).collect().sortBy(x => -x._1)
    val rules_ordered = rules_origin.collect().sorted(length_confidence) // 排过序的规则；
//    val antecedent_count_sum = (0 to antecedent_length_count.length).map(x => antecedent_length_count.map(_._2).slice(0, x).sum)
//    val real_length = rules_ordered.map(x => x.antecedent.length).distinct.sortBy(x => -x)
//    val length_rules_ordered = real_length.zip(antecedent_count_sum.map(x => rules_ordered.slice(x, rules_ordered.length)).slice(0,antecedent_count_sum.length-1) ).toMap

    validate_for_test.map(visited => getRecommend(visited, rules_ordered,recNum))
  }


  def getRecommend(visited: (Seq[String],Seq[String]), rules_ordered: Array[Rule[String]], recNum: Int):(Seq[String],
    Seq[String], Array[(String, Double)])  = {
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
    (visited._1,visited._2,buff.toArray)
  }

  def getWithRecommendWithInOut(rules_origin: RDD[AssociationRules.Rule[String]], validate_for_test: RDD[(Seq[String],Seq[String])], recNum: Int)
  : RDD[(Seq[String], Seq[String], Array[(String, Double)])] = {
    // 1. 规则排序
    val rules_ordered = rules_origin.collect().sorted(length_confidence) // 排过序的规则；
    validate_for_test.map(visited => getRecommend(visited, rules_ordered,recNum))
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
  def evalute(predictResult: RDD[(Seq[String], Seq[String], Array[(String, Double)])]): (Double, Double, Double, Int, Double, Double, Double) = {
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

    (precision, recall, fMeasure, precision_recall_fMeasure._1, precision_recall_fMeasure._2._1, precision_recall_fMeasure._2._2, precision_recall_fMeasure._2._3)
  }


  /**
    * 分割第一个访问页面为输入，后续interval分钟内的数据作为输出
    * @param data
    * @param interval
    * @return
    */
  def generateInputOutput(data: DataFrame, interval: Int):DataFrame ={
    val uidCol = "userid"
    val urlCol = "fullurl"
    val time_url = "time_url"
    val time_url_list = "time_url_list"
    val timeCol = "timestamp_format"
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val split_in_out = udf{(time_url_arr:Seq[org.apache.spark.sql.Row], interval:Int) =>
      val time_url_list = time_url_arr.map(x => (x.getString(0),x.getString(1))).sortBy(x => x._1)
      val final_interval = interval * 60 * 1000
      val first = time_url_list.head // first._2 is the input
      val second = time_url_list.tail.takeWhile(p =>
        (simpleDateFormat.parse(p._1).getTime - simpleDateFormat.parse(first._1).getTime) <= final_interval)
        .map(p => p._2)
      Array(Seq(first._2),second)
    }
    data.select(col(uidCol),struct(col(timeCol),col(urlCol)) as time_url)
      .groupBy(uidCol).agg(collect_list(time_url) as time_url_list).withColumn(time_url_list,split_in_out(col(time_url_list),lit(interval))).select(col(uidCol),col(time_url_list).getItem(0).as("urls_in"),col(time_url_list).getItem(1).as("urls_out"))
  }

}
