package com.tipdm.analyse.ForFP.fp_model

import com.tipdm.util.CommonUtil._
import org.apache.spark.mllib.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

object FPModelTest {
  /**
    * 训练FP模型
    *
    * @param table law_init1.data_101003_url_gt_one
    * @return
    */
  def createModel(table: String, sqlContext: HiveContext) = {

    val data = sqlContext.sql("select * from " + table).distinct.groupBy("userid").agg(collect_set("url") as "url_sets").filter("size(url_sets)>1")
    val train = data.select("url_sets").rdd.map(row => row.getSeq[String](0)).map(x => x.toArray)
    train.cache
    val minSupport = 0.00001
    val fpg = new FPGrowth().setMinSupport(minSupport).setNumPartitions(4)
    val model = fpg.run(train)
    model
  }

  /**
    * 生成规则
    *
    * @param model
    * @param minConfidence
    */
  def predictModel(model: FPGrowthModel[String], minConfidence: Double) = {
    model.generateAssociationRules(minConfidence).sortBy(x =>
      -x.confidence).take(30).foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent.mkString("[", ",", "]")
          + ", " + rule.confidence)
    }
  }

  def main(args: Array[String]): Unit = {
    val sqlContext = getSparkSession("FP model")
    val model = createModel(table_101003_gt_one, sqlContext)
    predictModel(model, 0.3)
  }
}
