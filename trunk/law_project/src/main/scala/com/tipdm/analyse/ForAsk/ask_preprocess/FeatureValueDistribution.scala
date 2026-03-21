package com.tipdm.analyse.ForAsk.ask_preprocess

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** suling
  * 统计特征字段的值的分布情况
  */
object FeatureValueDistribution {

  def valueDistribution(data: DataFrame, column_name: String, num: Int) = {
    val cal_udf = udf { (value: Double) => (value / num).toInt }
    val data_value_distribute = data.select(col(userid), cal_udf(col(column_name)) as column_name).groupBy(column_name).agg(count(col(userid)) as "ucount", count(col(userid)) * 100.0 / 2998 as "u_per").orderBy(desc("ucount"))
    data_value_distribute.show(20, false)
  }

  def main(args: Array[String]): Unit = {
    val data = ReadDB.getData()
    val features_data = ConstructFeatures.getConstructFeatures(data)
    val features_label_data = SplitFeatureWithLabel.split(features_data)
    valueDistribution(features_label_data, visit_duration, 100)
    valueDistribution(features_label_data, visit_count, 10)
    valueDistribution(features_label_data, visit_relevant_count, 10)

  }
}
