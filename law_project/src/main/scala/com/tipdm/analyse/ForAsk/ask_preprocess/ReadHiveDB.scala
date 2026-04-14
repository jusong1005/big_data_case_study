package com.tipdm.analyse.ForAsk.ask_preprocess

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame

/** suling
  * 读取Hive数据库中全量数据
  */
object ReadHiveDB {

  def getData(): DataFrame = {
    val hiveContext = getSparkSession("read data")
    hiveContext.sql("select userid,timestamp_format,fullurl,fullurlid,pagetitle,pagetitlecategoryid,pagetitlecategoryname,pagetitlekw from law_init1.lawtime_gt_one_distinct")
  }

  def main(args: Array[String]): Unit = {
    val data = getData()
    println("数据量：" + data.count())
    data.show(20)

  }
}
