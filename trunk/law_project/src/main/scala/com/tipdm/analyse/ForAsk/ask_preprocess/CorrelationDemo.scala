package com.tipdm.analyse.ForAsk.ask_preprocess

import com.tipdm.util.CommonUtil.{label, visit_count, visit_duration, visit_relevant_count}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.sql.DataFrame

/**
  * 特征列与目标类之间的相关性分析
  */
object CorrelationDemo {
  def corrDemo(data: DataFrame) = {
    val rdd = data.select(visit_duration, visit_count, visit_relevant_count, label).map(x => Vectors.dense(x.getDouble(0), x.getDouble(1), x.getDouble(2), x.getDouble(3)))
    Statistics.corr(rdd, "pearson")
  }

  def main(args: Array[String]): Unit = {
    val data = ReadDB.getData()
    val features_data = ConstructFeatures.getConstructFeatures(data)
    val features_label_data = SplitFeatureWithLabel.split(features_data)
    val features_change = DataExchange.change(features_label_data)
    val filtered_data = FilterData.filterData(features_change)
    val result = corrDemo(filtered_data)
    val cols = result.numCols
    val rows = result.numRows
    println(visit_duration + "\t" + visit_count + "\t" + visit_relevant_count + "\t" + label)
    for (i <- 0 until rows) {
      for (j <- 0 until cols) {
        print(result.apply(i, j) + "\t")
      }
      println()
    }
  }
}
