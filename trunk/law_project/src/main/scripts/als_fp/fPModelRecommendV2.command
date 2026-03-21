spark-shell --total-executor-cores 20 --executor-memory 5G --name fansy

val validateTable = "law_init1.data_101003_encoded_validate"
val uidCol = "userid"
val timeCol = "timestamp_format"
import org.apache.spark.sql.functions._
val interval_count = "interval_count"
val interval_count_percent = "interval_count_percent"
val time_interval = "time_interval"
import java.text.SimpleDateFormat

val pid_time = "pid_time_col"
val pid_time_list = "pid_time_col_list"

import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

val URL_SETS_COLUMN = "URL_SETS_COLUMN"
val trainTable = "law_init1.data_101003_encoded_train"
val validateTable = "law_init1.data_101003_encoded_validate"
val uidCol = "uid"
val pidCol = "pid"
import org.apache.spark.mllib.fpm.FPGrowth
val minSupport = 0.00001
val minConfidence = 0.2

///////////////////////////////////////////////////////////////////////////////////////////////////////////
val validate = sqlContext.sql("select * from "+validateTable)
val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
val getInterval = udf{(times:Seq[String]) =>
  val times_sorted = times.sorted
  (simpleDateFormat.parse(times_sorted.apply(1)).getTime - simpleDateFormat.parse(times_sorted.apply(0)).getTime)/1000
}

val first_filtered = validate.select(uidCol,timeCol).groupBy(uidCol).agg(collect_list(timeCol) as time_interval).filter(size(col(time_interval)) > 1 ).withColumn(time_interval,getInterval(col(time_interval)))

 val second_filtered = first_filtered.filter(time_interval + " > 0") // 不等于0
val valid_count = second_filtered.count // 385568

// 两次间隔 1~30分钟内的 访问用户占比
(1 to 30).map(x => (x, (second_filtered.filter(time_interval +" <= "+ x * 60).count()*100.0/valid_count).formatted("%.2f") + "% ")).foreach(println(_))
(1,30.85% )
(2,42.91% )
(3,49.77% )
(4,54.47% )
(5,58.02% )
(6,60.84% )
(7,63.08% )
(8,64.98% )
(9,66.62% )
(10,68.00% )
(11,69.22% )
(12,70.33% )
(13,71.31% )
(14,72.20% )
(15,73.02% )
(16,73.81% )
(17,74.51% )
(18,75.19% )
(19,75.81% )
(20,76.44% )
(21,77.06% )
(22,77.71% )
(23,78.39% )
(24,79.11% )
(25,79.84% )
(26,80.66% )
(27,81.47% )
(28,82.20% )
(29,82.86% )
(30,83.42% )

(0 to 29).zip(1 to 30).map(x => (x._1+"~"+x._2, (second_filtered.filter(time_interval +" < "+ x._2 * 60 +" and " + time_interval +" >= " + x._1 * 60 ).count()*100.0/valid_count).formatted("%.2f") + "% ")).foreach(println(_))
(0~1,30.55%)
(1~2,12.22% )
(2~3,6.91% )
(3~4,4.72% )
(4~5,3.57% )
(5~6,2.83% )
(6~7,2.25% )
(7~8,1.90% )
(8~9,1.64% )
(9~10,1.38% )
(10~11,1.22% )
(11~12,1.12% )
(12~13,0.98% )
(13~14,0.89% )
(14~15,0.82% )
(15~16,0.80% )
(16~17,0.70% )
(17~18,0.68% )
(18~19,0.62% )
(19~20,0.63% )
(20~21,0.62% )
(21~22,0.65% )
(22~23,0.68% )
(23~24,0.72% )
(24~25,0.72% )
(25~26,0.82% )
(26~27,0.81% )
(27~28,0.73% )
(28~29,0.66% )
(29~30,0.57% )
/////////////////////////////////////////////////////////////////////////////////






def generateRules(sc: SparkContext, sqlContext: SQLContext, trainTable: String,
                    uidCol: String, pidCol: String,
                    minSupport: Double, minConfidence: Double) = {
    // 1. 过滤只访问过一个url的用户数据，并把所有用户访问过的url进行集合（set）
    val data = sqlContext.sql("select " + uidCol + " , " + pidCol + " from " + trainTable).distinct.groupBy(uidCol).agg(collect_set(col(pidCol)) as URL_SETS_COLUMN).filter(size(col(URL_SETS_COLUMN)) > 1)

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

case class MyRule(antecedent: Set[Int], consequent: Int, confidence: Double)

 def getRecommend(first_visited: Int, rules: Array[MyRule], recNum: Int): Array[(Int,Double)] = {
    val buff = new ArrayBuffer[(Int,Double)]()
    for (rule <- rules) {
      if (rule.antecedent.contains(first_visited) ) {
        // 前缀全匹配
        buff.append((rule.consequent, rule.confidence))
        if (buff.length >= recNum) {
          return buff.toArray
        }
      }
    }
    buff.toArray
  }

def getWithRecommend(rules_origin: RDD[AssociationRules.Rule[Int]], validate_for_test: RDD[(Int, Int)], recNum: Int)
  : RDD[(Int, Array[(Int,Double)])] = {
    // 1. 规则排序

    val antecedent_length_count = rules_origin.map(x => (x.antecedent.length, 1)).reduceByKey((x, y) => x + y).collect().sortBy(x => -x._1)
    val rules_ordered = rules_origin.collect().sorted(length_confidence).map((x: AssociationRules.Rule[Int]) => MyRule(x.antecedent.toSet, x.consequent.head.toInt, x.confidence)) // 排过序的规则；

    val antecedent_count_sum = (0 to antecedent_length_count.length).map(x => antecedent_length_count.map(_._2).slice(0, x).sum)

    // 2. 整合validate和train数据
    val all_rules_ordered = antecedent_count_sum.map(x => rules_ordered.slice(x, rules_ordered.length))

    validate_for_test
      .map { x =>
        ((x._2, getRecommend(x._1, all_rules_ordered(antecedent_length_count.length - 1), recNum)))
      }
  }
def evalute(predictResult: RDD[(Int, Array[(Int,Double)])]): (Double, Double, Double, Int, Double, Double, Double) = {
    val tp_fp_fn = predictResult.map { x =>
      val realItems = x._1
      val recItems = x._2.map(_._1)
      if(recItems.size > 0 && recItems.contains(realItems)){// 推荐的看了
        (1.0, recItems.length - 1.0 , 0.0)
      }else{
        (0.0,recItems.length.toDouble,0.0)
      }
    }

    val precision_recall_fMeasure = tp_fp_fn.map[(Double, Double, Double)] { x =>
      val precision = if(x._1==0.0 && x._2 == 0.0) 0.0 else x._1 / (x._1 + x._2)
      val recall = if(x._1==0.0 && x._3 == 0.0) 0.0 else x._1 / (x._1 + x._3)
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

def generateTestData(sqlContext: SQLContext, validateTable: String, uidCol: String, pidCol: String, timeCol: String, valid_minutes: Int) = {
    val validate = sqlContext.sql(" select * from " + validateTable)
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val get_interval_data = udf { (pid_time_list_row: Seq[Row]) =>
      val pid_time_list = pid_time_list_row.map(row => (row.getLong(0),row.getString(1)))
      if (pid_time_list.length == 1) {
        (pid_time_list.head._1.toInt, -1) // 看过一个后，没有任何看过的URL，保留，但是不删除；
      } else {
        val sorted_p_t_l = pid_time_list.sortBy(x => x._1)
        val first_time = simpleDateFormat.parse(sorted_p_t_l.apply(0)._2).getTime
        val second_time = simpleDateFormat.parse(sorted_p_t_l.apply(1)._2).getTime
        if ((second_time - first_time) == 0) {
          // 第一个URL和第二个URL的访问间隔为0， 需要删除
          (sorted_p_t_l.head._1.toInt, -2)
        } else if ((second_time - first_time) < valid_minutes * 60) {
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
    val first_validate = validate.select(col(uidCol), struct(col(pidCol), col(timeCol)) as pid_time).groupBy(uidCol).agg(collect_list(col(pid_time)) as pid_time_list).withColumn(pid_time_list, get_interval_data(col(pid_time_list)))
      .select(get_first(col(pid_time_list)), get_second(col(pid_time_list))).rdd.map(row => (row.getInt(0), row.getInt(1))).filter(x => x._2 != -2)
    first_validate
  }


    val rules = generateRules(sc, sqlContext, trainTable, uidCol, pidCol, minSupport, minConfidence)

    val valid_minutes = 6
    // 分钟
   val validate_for_test = generateTestData(sqlContext, validateTable, uidCol, pidCol, timeCol, valid_minutes)
//scala> validate_for_test.count
//res6: Long = 490380
//validate_for_test.filter(x => x._2 == -1 ).count
//res7: Long = 290211

    val recNum = 3
    val recommended = getWithRecommend(rules, validate_for_test, recNum)
    // 4. 评估
    val (p, r, f, size, p_sum, r_sum, f_sum) = evalute(recommended)
    println("precision :" + p + " \t recall: " + r + " \t F1:" + f + " \t size :" + size)

//////////////////////

spark-submit --total-executor-cores 20 --executor-memory 5G --name fp_v2 --class com.tipdm.als_fp.FPModelRecommendV2 --files hdfs://server1:8020/user/root/hive-site.xml als_fp.jar law_init1.data_101003_encoded_train law_init1.data_101003_encoded_validate uid pid timestamp_format 0.00001 0.2 1,2,3,4,5,6 fp_v2_app 1,2,3,4,5,6,7,8,9,10