package com.tipdm.als_fp

import java.text.SimpleDateFormat

import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * FP 模型的关联规则的推荐 Version 2
  * 针对验证提出不同的思路：
  * 主要思想：针对用户访问一个URL后进行推荐；
  * 以此推荐结果来验证Precision和Recall，验证时，只取看的最近的一个URL
  * //@Author: fansy 
  * //@Time: 2018/10/29 15:53
  * //@Email: fansy1990@foxmail.com
  */
object FPModelRecommendV2 {

  val URL_SETS_COLUMN = "URL_SETS_COLUMN"
  val interval_count = "interval_count"
  val interval_count_percent = "interval_count_percent"
  val time_interval = "time_interval"

  val pid_time = "pid_time_col"
  val pid_time_list = "pid_time_col_list"

  def generateRules(sc: SparkContext, sqlContext: SQLContext, trainTable: String,
                    uidCol: String, pidCol: String,
                    minSupport: Double, minConfidence: Double) = {
    // 1. 过滤只访问过一个url的用户数据，并把所有用户访问过的url进行集合（set）
    val data = sqlContext.sql("select " + uidCol + " , " + pidCol + " from " + trainTable)
      .distinct.groupBy(uidCol).agg(collect_set(col(pidCol)) as URL_SETS_COLUMN).filter(size(col(URL_SETS_COLUMN)) > 1)

    // 2. 把数据构建成 FPModel需要的数据
    val train = data.select(URL_SETS_COLUMN).rdd.map(row => row.getSeq[Long](0)).map(x => x.map(_.toInt).toArray)
    train.cache

    // 3.
    val fpg = new FPGrowth().setMinSupport(minSupport)
    val model = fpg.run(train)
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
    * @param first_visited
    * @param rules : 和 validate_visited 中url个数匹配的规则
    * @param recNum
    * @return
    */
  def getRecommend(first_visited: Int, rules: Array[MyRule], recNum: Int): Array[(Int, Double)] = {
    val buff = new ArrayBuffer[(Int, Double)]()
    for (rule <- rules) {
      if (rule.antecedent.contains(first_visited)) {
        // 前缀全匹配
        buff.append((rule.consequent, rule.confidence))
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
    * @param validate_for_test
    * @param recNum
    * @return
    */
  def getWithRecommend(rules_origin: RDD[AssociationRules.Rule[Int]], validate_for_test: RDD[(Int, Int)], recNum: Int)
  : RDD[(Int, Array[(Int, Double)])] = {
    // 1. 规则排序
    val antecedent_length_count = rules_origin.map(x => (x.antecedent.length, 1)).reduceByKey((x, y) => x + y).collect().sortBy(x => -x._1)
    val rules_ordered = rules_origin.collect().sorted(length_confidence)
      .map((x: AssociationRules.Rule[Int]) => MyRule(x.antecedent.toSet, x.consequent.head.toInt, x.confidence)) // 排过序的规则；
    val antecedent_count_sum = (0 to antecedent_length_count.length).map(x => antecedent_length_count.map(_._2).slice(0, x).sum)

    // 2. 整合validate和train数据
    val all_rules_ordered = antecedent_count_sum.map(x => rules_ordered.slice(x, rules_ordered.length))
    validate_for_test
      .map { x =>
        ((x._2, getRecommend(x._1, all_rules_ordered(antecedent_length_count.length - 1), recNum)))
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
  def evalute(predictResult: RDD[(Int, Array[(Int, Double)])]): (Double, Double, Double, Int, Double, Double, Double) = {
    val tp_fp_fn = predictResult.map { x =>
      val realItems = x._1
      val recItems = x._2.map(_._1)
      if (recItems.size > 0 && recItems.contains(realItems)) { // 推荐的看了
        (1.0, recItems.length - 1.0, 0.0)
      } else {
        (0.0, recItems.length.toDouble, 0.0)
      }
    }

    val precision_recall_fMeasure = tp_fp_fn.map[(Double, Double, Double)] { x =>
      val precision = if (x._1 == 0.0 && x._2 == 0.0) 0.0 else x._1 / (x._1 + x._2)
      val recall = if (x._1 == 0.0 && x._3 == 0.0) 0.0 else x._1 / (x._1 + x._3)
      (precision, recall, if (precision == 0.0 && recall == 0.0) 0.0 else 2 * precision * recall / (precision + recall))
    }.map(x => (1, x)).reduce((x1, x2) =>
      (x1._1 + x2._1, (x1._2._1 + x2._2._1, x1._2._2 + x2._2._2, x1._2._3 + x2._2._3)))
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
      .append(" <timeCol> ")
      .append(" <minSupport> ")
      .append(" <minConfidence> ")
      .append(" <recNum1,recNum2> ")
      .append(" <appName> ")
      .append(" <valid_minutes1,valid_minutes2> ")
  }

  def handle_args(args: Array[String]) = {
    val trainTable = args(0)
    val validateTable = args(1)
    val uidCol = args(2)
    val pidCol = args(3)
    val timeCol = args(4)
    val minSupport = args(5).toDouble
    val minConfidence = args(6).toDouble
    val recNum = args(7).split(",").map(_.toInt)
    val appName = args(8)
    val valid_minutes = args(9).split(",").map(_.toDouble)
    (trainTable, validateTable, uidCol, pidCol, timeCol, minSupport, minConfidence, recNum, appName, valid_minutes)
  }

  /**
    * 处理validate数据，只保留，每个用户6分钟内访问的记录，并反复这个用户第一次访问的记录及第二次访问的记录（可能没有访问）；
    * 排除掉前后两个URL访问间隔为0的记录；
    *
    * @param sqlContext
    * @param validateTable
    * @param uidCol
    * @param pidCol
    * @param timeCol
    * @param valid_minutes
    */
  def generateTestData(sqlContext: SQLContext, validateTable: String, uidCol: String, pidCol: String, timeCol: String, valid_minutes: Double) = {
    val validate = sqlContext.sql(" select * from " + validateTable)
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val get_interval_data = udf { (pid_time_list_row: Seq[Row]) =>
      val pid_time_list = pid_time_list_row.map(row => (row.getLong(0), row.getString(1)))
      if (pid_time_list.length == 1) {
        (pid_time_list.head._1.toInt, -1) // 看过一个后，没有任何看过的URL，保留，但是不删除；
      } else {
        val sorted_p_t_l = pid_time_list.sortBy(x => x._2) // 按照时间排序
        val first_time = simpleDateFormat.parse(sorted_p_t_l.apply(0)._2).getTime
        val second_time = simpleDateFormat.parse(sorted_p_t_l.apply(1)._2).getTime
        if ((second_time - first_time) == 0) {
          // 第一个URL和第二个URL的访问间隔为0， 需要删除
          (sorted_p_t_l.head._1.toInt, -2)
        } else if ((second_time - first_time) < valid_minutes * 60 * 1000) {
          //符合条件
          (sorted_p_t_l.head._1.toInt, sorted_p_t_l.apply(1)._1.toInt)
        } else {
          // 访问一个URL后，在6分钟内并没有访问的记录，保留，但是不删除
          (sorted_p_t_l.head._1.toInt, -1)
        }
      }
    }
    val get_first = udf { (x: Row) => x.getInt(0) }
    val get_second = udf { (x: Row) => x.getInt(1) }
    val first_validate = validate.select(col(uidCol), struct(col(pidCol), col(timeCol)) as pid_time)
      .groupBy(uidCol).agg(collect_list(col(pid_time)) as pid_time_list).withColumn(pid_time_list, get_interval_data(col(pid_time_list)))
      .select(get_first(col(pid_time_list)), get_second(col(pid_time_list)))
      .rdd.map(row => (row.getInt(0), row.getInt(1))).filter(x => x._2 != -2)
    first_validate
  }


  def main(args: Array[String]): Unit = {
    if (args.length != 10) {
      printUsage()
      System.exit(1)
    }
    // 0. 参数处理
    val (trainTable, validateTable, uidCol, pidCol, timeCol, minSupport, minConfidence, recNums, appName, valid_minutes) = handle_args(args)

    // 1. 初始化
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    // 2. 生成规则
    val rules = generateRules(sc, sqlContext, trainTable, uidCol, pidCol, minSupport, minConfidence)

    // 3. 推荐, 只保留 validateTable中每个用户访问后，6分钟内的访问记录；然后推荐1个；所以保留每个用户最多访问2个URL，最少访问1个URL
    //  val valid_minutes = 6分钟
    println("recNum \t valid_minute \t precision \t reacall \t F1-Measure \t size \t p_sum \t r_sum \t f_sum")
    for (recNum <- recNums; valid_minute <- valid_minutes) {
      val validate_for_test = generateTestData(sqlContext, validateTable, uidCol, pidCol, timeCol, valid_minute)
      //  val recNum = 3
      val recommended = getWithRecommend(rules, validate_for_test, recNum)
      //  4. 评估
      val (p, r, f, size, p_sum, r_sum, f_sum) = evalute(recommended)
      println(recNum + " \t " + valid_minute + " \t " + p.formatted("%.4f") + "% \t " + r.formatted("%.4f")
        + "% \t " + f.formatted("%.4f") + "% \t " + size + " \t " + p_sum + " \t " + r_sum + " \t " + f_sum)
    }

    sc.stop()
  }

  /**
    * 统计每个用户2次访问的时间间隔，得到阈值
    *
    * @param sc
    * @param sqlContext
    * @param validateTable
    * @param uidCol
    * @param timeCol
    */
  def statics(sc: SparkContext, sqlContext: SQLContext, validateTable: String, uidCol: String, timeCol: String) = {
    val validate = sqlContext.sql("select * from " + validateTable)
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val getInterval = udf { (times: Seq[String]) =>
      val times_sorted = times.sorted
      (simpleDateFormat.parse(times_sorted.apply(1)).getTime - simpleDateFormat.parse(times_sorted.apply(0)).getTime) / 1000
    }

    val first_filtered = validate.select(uidCol, timeCol).groupBy(uidCol).agg(collect_list(timeCol) as time_interval)
      .filter(size(col(time_interval)) > 1).withColumn(time_interval, getInterval(col(time_interval)))

    val second_filtered = first_filtered.filter(time_interval + " > 0")
    // 不等于0
    val valid_count = second_filtered.count // 385568

    // 两次间隔 1~30分钟内的 访问用户占比
    (1 to 30).map(x => (x, (second_filtered.filter(time_interval + " <= " + x * 60).count() * 100.0 / valid_count)
      .formatted("%.2f") + "% ")).foreach(println(_))

    (1 to 29).zip(2 to 30).map(x => (x._1 + "~" + x._2, (second_filtered.filter(time_interval
      + " < " + x._2 * 60 + " and " + time_interval + " >= " + x._1 * 60).count() * 100.0 / valid_count)
      .formatted("%.2f") + "% ")).foreach(println(_))
  }
}
