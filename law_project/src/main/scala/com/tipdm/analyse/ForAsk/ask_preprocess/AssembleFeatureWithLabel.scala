package com.tipdm.analyse.ForAsk.ask_preprocess

import com.tipdm.util.CommonUtil._
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame

/**
  * //@Author: fansy 
  * //@Time: 2018/9/21 9:49
  * //@Email: fansy1990@foxmail.com
  * 合并所有特征列
  */
object AssembleFeatureWithLabel {

  def assemble(data: DataFrame): DataFrame = {
    // assemble features
    val assembler = new VectorAssembler()
      .setInputCols(Array(visit_duration, visit_count, visit_relevant_count))
      .setOutputCol(assembled_features)
    val assemble_features_label = assembler.transform(data)
    assemble_features_label
  }

  def assemble2(data: DataFrame): DataFrame = {
    // assemble features
    val assembler = new VectorAssembler()
      .setInputCols(Array(visit_duration, visit_count, visit_relevant_count, visit_relevant_count2, label))
      .setOutputCol("assembled2_features")
    val assemble_features_label = assembler.transform(data)
    assemble_features_label
  }

  def main(args: Array[String]): Unit = {

    val data = ReadDB.getData()
    val features_data = ConstructFeatures.getConstructFeatures(data)
    val features_label_data = SplitFeatureWithLabel.split(features_data)
    val filtered_data = FilterData.filterData(features_label_data)
    val assembled_data = assemble(filtered_data)
    assembled_data.show(3)
  }
}
