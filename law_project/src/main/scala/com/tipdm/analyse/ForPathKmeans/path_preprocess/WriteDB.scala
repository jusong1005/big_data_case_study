package com.tipdm.analyse.ForPathKmeans.path_preprocess

import java.util.Properties

import com.tipdm.analyse.ForPathKmeans.readOrwriteData.ReadHiveData
import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * suling
  * 写入数据到mysql
  */
object WriteDB {
  def writeData(data: DataFrame, table: String) = {
    val url = "jdbc:mysql://localhost:3306/law_init?useUnicode=true&characterEncoding=utf8"
    val user = "root"
    val password="123456"
    val driver = "com.mysql.jdbc.Driver"
    val properties = new Properties()
    properties.put("user", user)
    properties.put("password", password)
    properties.put("driver", driver)
    data.write.mode(SaveMode.Append).jdbc(url, table, properties)
  }

  def main(args: Array[String]): Unit = {
    val data = ReadHiveData.readData(table_gt_oneclick_data_distinct)
    val data_hunyin = OnlyHunyin.getData(data, "info/hunyin")
    writeData(data_hunyin, "law_init.lawtime_hunyin")
  }
}
