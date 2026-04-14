package com.tipdm.analyse.ForAsk.ask_preprocess

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame

/** created by suling
  * 用于将指定数量的0类数据与1类数据合并
  */
object UnionData {

  def unionData(data: DataFrame): DataFrame = {
    val data1 = data.filter(label + "=1")
    val size = data1.count() * 2
    val data0 = data.filter(label + "=0").limit(size.toInt)
    data0.unionAll(data1)
  }

  def main(args: Array[String]): Unit = {
    val data = ReadDB.getData()
    val features_data = ConstructFeatures.getConstructFeatures(data)
    val features_label_data = SplitFeatureWithLabel.split(features_data)
    println("1类数据的数量：" + features_label_data.filter("label=1").count())
    println("0类数据的数量：" + features_label_data.filter("label=0").count())
    val union_data = unionData(features_label_data)
    println("记录数" + union_data.count())
  }
}
