package com.tipdm.analyse.ForPathKmeans.path_preprocess

import com.tipdm.analyse.ForPathKmeans.path_preprocess.DataExchange.setSQLContext
import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

object PathDistribution {
  def getArray(arr: Seq[String], m: Int): Seq[String] = {
    // 不定长轨迹转换为定长轨迹
    var intestArray = Array.fill(m)("null")
    for (i <- 0 until arr.length; if i < m) {
      intestArray(i) = arr(i)
    }
    return intestArray.toSeq
  }

  def statPath(data: DataFrame, sqlContext: HiveContext, m: Int) = {
    val separator = "\\|\\|\\|"
    sqlContext.udf.register("udf_getArray", (arr: Seq[String], m: Int) => getArray(arr, m))
    // 数组长度
    // 按指定分隔符拆分value列，生成splitCols列
    var newDF = data.withColumn("furls_arr", split(col("fullurls"), separator)).selectExpr(userid, "udf_getArray(furls_arr,28) as furls_sanme_arr")
    val numAttrs = m
    val attrs = Array.tabulate(numAttrs)(n => "col_" + n)
    attrs.zipWithIndex.foreach(x => {
      newDF = newDF.withColumn(x._1, col("furls_sanme_arr").getItem(x._2))
    })
    attrs.zipWithIndex.foreach(x => {
      newDF.groupBy(x._1).agg(count(x._1) as x._1 + "_distribution").orderBy(desc(x._1 + "_distribution")) show(50, false)
    })
  }

  def main(args: Array[String]): Unit = {
    val sqlContext = getSparkSession("data exchange")
    setSQLContext(sqlContext)
    val data = ReadDB.getData(sqlContext)
    val data_clean = DataClean.clean(data)
    val data_specificate = AttributeSpecification.specificate(data_clean, 5, sqlContext)
    println("记录数：" + data_specificate.count())
    println("用户数：" + data_specificate.select(userid).distinct().count())
    println("网页数：" + data_specificate.select(fullurl).distinct().count())
    val data_concat = ConnectURL.concat_url(data_specificate, fullurl, "fullurls", "\\|\\|\\|")
    val data_split = statPath(data_concat, sqlContext, 5)
  }
}
