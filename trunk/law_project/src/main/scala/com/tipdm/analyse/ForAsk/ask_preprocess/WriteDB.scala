package com.tipdm.analyse.ForAsk.ask_preprocess

import java.util.Properties
import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * suling
  * 写入数据到mysql
  *
  */
object WriteDB {
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
    val fullurl = "fullurl"
    val askSuccess_keywork = "askSuccess"
    val timestamp_format = "timestamp_format"
    val filter_data = data.filter(fullurl + " like '%" + askSuccess_keywork + "%'").select(userid).join(data, userid).orderBy(userid)
    writeData(filter_data, "law_fansy.lawtime_askSuccess")
  }
}
