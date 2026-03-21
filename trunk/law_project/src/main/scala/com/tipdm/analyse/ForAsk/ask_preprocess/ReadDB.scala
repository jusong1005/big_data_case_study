package com.tipdm.analyse.ForAsk.ask_preprocess

import java.util.Properties

import com.tipdm.util.CommonUtil
import org.apache.spark.sql.DataFrame

/**
  * //@Author: fansy 
  * //@Time: 2018/9/20 11:50
  * //@Email: fansy1990@foxmail.com
  */
object ReadDB {

  val url = "jdbc:mysql://192.168.111.75:3306/law_fansy?useUnicode=true&characterEncoding=utf8"
  val user = "root"
  val password = "root"
  val table = "law_fansy.gz_lawyer_data"
  val driver = "com.mysql.jdbc.Driver"

  def getData(): DataFrame = {
    val properties = new Properties()
    properties.put("user", user)
    properties.put("password", password)
    properties.put("driver", driver)
    CommonUtil.getSparkSession().read.jdbc(url, table, properties)
  }

  def main(args: Array[String]): Unit = {
    val data = getData()
    data.show(2)
    data.count()
  }

}
