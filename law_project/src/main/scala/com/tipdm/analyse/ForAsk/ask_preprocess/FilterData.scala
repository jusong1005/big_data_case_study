package com.tipdm.analyse.ForAsk.ask_preprocess

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame

/**
  * //@Author: fansy 
  * //@Time: 2018/9/21 12:49
  * //@Email: fansy1990@foxmail.com
  * 过滤掉其中 特征列全部为0，目标列为-1的数据
  */
object FilterData {

  def filterData(data: DataFrame): DataFrame = {
    filterData(data, false)
  }

  def filterData(data: DataFrame, showOrNot: Boolean): DataFrame = {
    val not_include_data = data.filter(label + " = " + "-1.0")
    if (showOrNot) {
      val count = not_include_data.count()
      println("Filtered data :" + count)
      if (count < 50) {
        not_include_data.show(count.toInt)
      } else {
        not_include_data.show(10)
      }
    }
    data.filter(label + "!=" + -1.0)
  }

  def main(args: Array[String]): Unit = {
    val data = ReadDB.getData()
    val features_data = ConstructFeatures.getConstructFeatures(data)
    //    println(features_data)
    val features_label_data = SplitFeatureWithLabel.split(features_data)
    val num1 = features_label_data.count()
    val filtered_data = filterData(features_label_data)
    val num2 = filtered_data.count()
    println("过滤的记录数：" + (num1 - num2))
  }
}
