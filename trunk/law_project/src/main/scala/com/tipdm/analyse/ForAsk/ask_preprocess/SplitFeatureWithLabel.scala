package com.tipdm.analyse.ForAsk.ask_preprocess

import java.util.Properties

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * //@Author: fansy 
  * //@Time: 2018/9/21 9:49
  * //@Email: fansy1990@foxmail.com
  * 分割特征列和目标列
  * 由于前一个步骤输出的时候特征列和目标列都在一个Vector中，所以需要进行分割；
  */
object SplitFeatureWithLabel {

  def split(data: DataFrame): DataFrame = {

    val elements = Array(visit_duration, visit_count, visit_relevant_count, label)
    val sqlExpr = elements.zipWithIndex.map { case (alias, idx) => col(features).getItem(idx).as(alias) }
    val de_features_label = data.select(col(userid) +: sqlExpr: _*)
    de_features_label
  }

  def writeData(data: DataFrame, table: String) = {
    val url = "jdbc:mysql://192.168.111.75:3306/law_fansy?useUnicode=true&characterEncoding=utf8"
    val user = "root"
    val password = "root"
    val driver = "com.mysql.jdbc.Driver"
    val properties = new Properties()
    properties.put("user", user)
    properties.put("password", password)
    properties.put("driver", driver)
    data.write.mode(SaveMode.Append).jdbc(url, table, properties)

  }

  def main(args: Array[String]): Unit = {
    val data = ReadDB.getData()
    val features_data = ConstructFeatures.getConstructFeatures(data)
    val features_label_data = split(features_data)
    features_label_data.show(3)

  }
}
