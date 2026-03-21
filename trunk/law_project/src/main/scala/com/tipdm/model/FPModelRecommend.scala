package com.tipdm.model

import com.tipdm.util.CommonUtil._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.collect_set
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object FPModelRecommend {

  case class MyRule(antecedent: Set[String], consequent: String, confidence: Double)

  case class MyRules(antecedent: Array[String], consequent: Array[String], confidences: Double)

  case class MyRating(uid: String, product: String, confidence: Double)

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
  def getRecommend(train_visited: Set[String], rules: Array[MyRule], recNum: Int): Array[MyRating] = {
    val buff = new ArrayBuffer[MyRating]()
    for (rule <- rules) {
      if (rule.antecedent.&(train_visited).size == rule.antecedent.size) {
        // 前缀全匹配
        if (!train_visited.contains(rule.consequent)) { // 推荐值不应出现在 train_visited中
          buff.append(MyRating("-1", rule.consequent, rule.confidence))
        }
        if (buff.length >= recNum) {
          return buff.toArray
        }
      }
    }
    buff.toArray
  }


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
    * @param recNum
    * @return
    */
  def getWithRecommend(rules_origin: RDD[MyRules], train: RDD[(String, Set[String])], recNum: Int) = {
    // 1. 规则排序
    val antecedent_length_count = rules_origin.map(x => (x.antecedent.length, 1)).reduceByKey((x, y) => x + y).collect().sortBy(x => -x._1)
    val rules_ordered = rules_origin.map(x => MyRule(x.antecedent.toSet, x.consequent.head.toString, x.confidences)).toArray() // 排过序的规则；
    val antecedent_count_sum = (0 to antecedent_length_count.length).map(x => antecedent_length_count.map(_._2).slice(0, x).sum)
    // 2. 整合validate和train数据
    val all_rules_ordered = antecedent_count_sum.map(x => rules_ordered.slice(x, rules_ordered.length))
    val recommend = train.map { x =>
      val url_size = if (x._2.size >= antecedent_length_count.head._1) antecedent_length_count.head._1 else x._2.size
      (x._1, getRecommend(x._2, all_rules_ordered(antecedent_length_count.length - url_size), recNum))
    }
    recommend
  }

  def encodeToFullUrl(data: DataFrame, usermeta: DataFrame, itemmeta: DataFrame, datauid: String,
                      datapid: String, metauid: String, metapid: String, metauser: String, metaitem: String) = {
    val data1 = data.join(usermeta, data(datauid) === usermeta(metauid), "inner")
    val data2 = data1.join(itemmeta, data1(datapid) === itemmeta(metapid), "inner")
    data2.select(metauser, metaitem)
  }

  def parseArgs(args: Array[String]) = {
    // 输入表，规则表，推荐个数,usercol,fullurlcol,推荐结果表
    (args(0), args(1), args(2), args(3), args(4), args(5))
  }

  def main(args: Array[String]): Unit = {
    val (inputTable, rulesTable, recNum, uidCol, pidCol, recommendPath) = parseArgs(args)
    // 初始化
    val sc = new SparkContext(new SparkConf().setAppName("FP Recommend").setExecutorEnv("SPARK_JAVA_OPTS", "-Xss4096k"))
    val sqlContext = new HiveContext(sc)
    val rules = sqlContext.sql("select antecedent,consequent,confidences from " + rulesTable).map(x => MyRules(x.getSeq[String](0).toArray, x.getSeq[String](1).toArray, x.getDouble(2)))
    val train = sqlContext.sql("select * from " + inputTable).select(uidCol, pidCol).groupBy(uidCol).agg(collect_set(pidCol)).rdd.repartition(24).map(row => (row.getString(0), (row.getSeq[String](1)).map(_.trim).toSet))
    val recommend = getWithRecommend(rules, train, recNum.toInt)
    val recommends = recommend.filter(_._2.length > 0).mapValues(x => x.sortWith((y1, y2) => y1.confidence > y2.confidence)).mapValues(x => x.map(y => y.product)).map(x => x._1 + "|||" + x._2.toSet.mkString("|||"))
    saveAsHDFSFile(recommends, recommendPath, true, sqlContext)

  }
}
