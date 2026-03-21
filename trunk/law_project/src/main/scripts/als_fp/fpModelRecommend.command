node2 :
spark-shell --total-executor-cores 20 --executor-memory 5G --name fansy

import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth}
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext
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

def generateRules(sc:SparkContext, sqlContext:SQLContext, trainTable:String,
                    uidCol:String,pidCol:String,
                    minSupport:Double, minConfidence:Double) ={
    // 1. 过滤只访问过一个url的用户数据，并把所有用户访问过的url进行集合（set）
    val data = sqlContext.sql("select "+ uidCol +" , " + pidCol+" from "+ trainTable).distinct.groupBy(uidCol).agg(collect_set(col(pidCol)) as URL_SETS_COLUMN).filter(size(col(URL_SETS_COLUMN)) > 1)

    // 2. 把数据构建成 FPModel需要的数据
    val train = data.select(URL_SETS_COLUMN).rdd.map(row => row.getSeq[Long](0)).map(x => x.map(_.toInt).toArray)
    train.cache

    // 3.
    val fpg = new FPGrowth().setMinSupport(minSupport)
    val model = fpg.run(train)
    println("freqItemSet count : "+ model.freqItemsets.count() )
    val rules = model.generateAssociationRules(minConfidence)
    train.unpersist()
    rules
  }
case class MyRule(antecedent:Set[Int],consequent:Int,confidence:Double)

def getRecommend(train_visited:Set[Int] ,rules: Array[MyRule],recNum:Int): Array[Rating] = {
    val buff = new ArrayBuffer[Rating]()
    for(rule <- rules){
      if(rule.antecedent.&(train_visited).size  == rule.antecedent.size) {// 前缀全匹配

        if(!train_visited.contains(rule.consequent)){ // 推荐值不应出现在 train_visited中
          buff.append(Rating(-1,rule.consequent,rule.confidence))
        }
        if(buff.length >= recNum){
          return buff.toArray
        }
      }
    }
    buff.toArray
  }

  val length_confidence = new Ordering[AssociationRules.Rule[Int]]{
    override def compare(x: Rule[Int], y: Rule[Int]): Int = if(x.antecedent.length != y.antecedent.length){
      y.antecedent.length - x.antecedent.length
    }else {
      if (- x.confidence + y.confidence > 0){
        1
      }else if(x.confidence == y.confidence){
        0
      }else{
        -1
      }
    }
  }

def getWithRecommend(rules_origin:RDD[AssociationRules.Rule[Int]],train:RDD[(Int,Set[Int])] ,validate:RDD[(Int,Set[Int])], recNum:Int)
  :RDD[(Int,(Array[Int], Array[Rating]))]={
    // 1. 规则排序

    val antecedent_length_count = rules_origin.map(x => (x.antecedent.length,1)).reduceByKey((x,y) => x+y).collect().sortBy(x => -x._1)
    val rules_ordered = rules_origin.collect().sorted(length_confidence).map((x:AssociationRules.Rule[Int]) => MyRule(x.antecedent.toSet,x.consequent.head.toInt,x.confidence)) // 排过序的规则；

    val antecedent_count_sum = (0 to antecedent_length_count.length).map(x => antecedent_length_count.map(_._2).slice(0,x).sum)

    // 2. 整合validate和train数据
    val all_rules_ordered = antecedent_count_sum.map(x => rules_ordered.slice( x, rules_ordered.length) )

    validate.join(train)
      .map{ x =>
        val url_size = if(x._2._1.size >= antecedent_length_count.head._1) antecedent_length_count.head._1 else x._2._1.size
        (x._1,(x._2._1.toArray,getRecommend(x._2._2 ,all_rules_ordered(antecedent_length_count.length - url_size),recNum)))
      }
  }
def evalute(predictResult: RDD[(Int, (Array[Int], Array[Rating]))]):(Double,Double,Double) = {
    val tp_fp_fn = predictResult.map{x =>
      val realItems = x._2._1.toSet
      val recItems = x._2._2.map(_.product).toSet
      ( realItems.&(recItems).size, recItems.diff(realItems).size, realItems.diff(recItems).size)
    }

    val precision_recall_fMeasure = tp_fp_fn.map[(Double,Double,Double)] { x =>
      val precision =if (x._1 == 0) 0.0 else x._1.toDouble / (x._1 + x._2)
      val recall =if (x._1 == 0) 0.0 else x._1.toDouble / (x._1 + x._3)
      (
        precision,recall,
        if(precision == 0.0 && recall == 0.0 ) 0.0 else 2 * precision * recall / (precision + recall)
      )
    }.map(x => (1, x)).reduce((x1, x2) =>
      (x1._1 + x2._1, (x1._2._1 + x2._2._1, x1._2._2 + x2._2._2, x1._2._3 + x2._2._3)))
    val precision = precision_recall_fMeasure._2._1 / precision_recall_fMeasure._1
    val recall = precision_recall_fMeasure._2._2 / precision_recall_fMeasure._1
    val fMeasure = precision_recall_fMeasure._2._3 / precision_recall_fMeasure._1

    (precision,recall,fMeasure,precision_recall_fMeasure._1,precision_recall_fMeasure._2._1,precision_recall_fMeasure._2._2,precision_recall_fMeasure._2._3)
  }

val recNum = 10
 val rules = generateRules(sc,sqlContext,trainTable,uidCol,pidCol,minSupport ,minConfidence )

    // 3. 推荐
val train = sqlContext.sql("select * from "+ trainTable).select(uidCol,pidCol).groupBy(uidCol).agg(collect_set(pidCol) ).rdd.repartition(24).map(row => (row.getLong(0).toInt,(row.getSeq[Long](1)).map(_.toInt).toSet ))
val validate = sqlContext.sql("select * from "+ validateTable).select(uidCol,pidCol).groupBy(uidCol).agg(collect_set(pidCol) ).rdd.repartition(8).map(row => (row.getLong(0).toInt,(row.getSeq[Long](1)).map(_.toInt).toSet ))
val recommended = getWithRecommend(rules,train,validate,recNum)
// 4. 评估
val (p,r,f, size, p_sum,r_sum,f_sum ) = evalute(recommended)
println("precision :"+ p + " \t recall: "+ r+" \t F1:"+ f+" \t size :"+ size)


p: Double = 0.019221965443860376
r: Double = 0.015142067004917156
f: Double = 0.015788849871256498