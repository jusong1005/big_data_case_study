package com.tipdm.analyse.ForAsk.ask_preprocess

import com.tipdm.util.CommonUtil._
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.DataFrame

/**
  * //@Author: fansy 
  * //@Time: 2018/9/21 11:26
  * //@Email: fansy1990@foxmail.com
  */
object ScaleData {

  def scale(data: DataFrame): DataFrame = {
    val scaler = new StandardScaler()
      .setInputCol(assembled_features)
      .setOutputCol(scaled_features)
      .setWithStd(true)
      .setWithMean(false)

    val scalerModel = scaler.fit(data)
    val scaledData = scalerModel.transform(data)
    scaledData
  }

  def scale2(data: DataFrame): DataFrame = {
    val scaler = new StandardScaler()
      .setInputCol("assembled2_features")
      .setOutputCol(scaled_features)
      .setWithStd(true)
      .setWithMean(true)
    //  归一化后的数据
    val scaled_data = scaler.fit(data).transform(data)
    scaled_data
  }

  def main(args: Array[String]): Unit = {
    val data = ReadDB.getData()
    val features_data = ConstructFeatures.getConstructFeatures(data)
    val features_label_data = SplitFeatureWithLabel.split(features_data)
    features_label_data.show(3)
    val filtered_data = FilterData.filterData(features_label_data)
    val assembled_data = AssembleFeatureWithLabel.assemble(filtered_data)
    assembled_data.show(2)
    val scaled_data = scale(assembled_data)
    scaled_data.show(20, false)
  }
}
