package com.tipdm.analyse.ForAsk.ask_preprocess

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * suling
  * 将数据根据分段函数进行转换
  */
object DataExchange {

  def fun_visit_duration(visit_duration: Double): Double = {
    if (visit_duration < 100) return visit_duration
    else return 100
  }

  def fun_visit_count(visit_count: Double): Double = {
    if (visit_count <= 20) return visit_count
    else if (visit_count > 20 && visit_count < 50) return (visit_count - 20) / 30.0 * 5 + 20
    else return 25
  }

  def fun_relevant_count(relevant_count: Double): Double = {
    if (relevant_count <= 5) return relevant_count
    else return 5
  }

  def change(data: DataFrame): DataFrame = {
    val udf_visit_duration = udf { (visit_duration: Double) => fun_visit_duration(visit_duration) }
    val udf_visit_count = udf { (visit_count: Double) => fun_visit_count(visit_count) }
    val udf_relevant_count = udf { (relevant_count: Double) => fun_relevant_count(relevant_count) }
    val data_change = data.select(col(userid), udf_visit_duration(col(visit_duration)) as visit_duration, udf_visit_count(col(visit_count)) as visit_count, udf_relevant_count(col(visit_relevant_count)) as visit_relevant_count, col(label))
    data_change
  }

  def main(args: Array[String]): Unit = {
    val data = ReadDB.getData()
    val features_data = ConstructFeatures.getConstructFeatures(data)
    val features_label_data = SplitFeatureWithLabel.split(features_data)
    val data_change = change(features_label_data)
    data_change.show(20, false)
  }
}
