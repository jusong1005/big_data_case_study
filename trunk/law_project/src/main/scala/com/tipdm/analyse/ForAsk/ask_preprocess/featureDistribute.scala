package com.tipdm.analyse.ForAsk.ask_preprocess

import org.apache.spark.sql.DataFrame

/** suling
  * 统计各字段的数据分布
  */
object featureDistribute {

  def distribute(data: DataFrame): DataFrame = {
    val distribute = data.describe()
    distribute
  }

  def main(args: Array[String]): Unit = {
    val data = ReadDB.getData()
    val featrues_data = ConstructFeatures.getConstructFeatures(data)
    val features_label_data = SplitFeatureWithLabel.split(featrues_data)
    val features_label_describe = distribute(features_label_data)
    features_label_describe.show(false)
  }
}
