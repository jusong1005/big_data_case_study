package com.tipdm.dataclean

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/** suling
  * 用于将同一个用户的访问记录进行合并
  */
object DataPreprocess03_FP {
  def getFPData(data: DataFrame, uidCol: String, pidCol: String, newCol: String) = {
    // 1. 过滤只访问过一个url的用户数据，并把所有用户访问过的url进行集合（set）
    val data2 = data.select(uidCol, pidCol).distinct
      .groupBy(uidCol).agg(collect_set(col(pidCol)) as newCol).filter(size(col(newCol)) > 1)

    // 2. 把数据构建成 FPModel需要的数据
    val train = data2.select(newCol) //.rdd.map(row => row.getSeq[Long](0)).map(x => x.map(_.toInt).toArray).map(x=>x.mkString(","))
    train
  }

  def parseArgs(args: Array[String]) = {
    //输入表、用户ID、网址ID、组合后的字段名输出文件路径
    (args(0), args(1), args(2), args(3), args(4))
  }

  def main(args: Array[String]): Unit = {
    val (inputTable, uidCol, pidCol, newCol, output) = parseArgs(args)
    // 初始化
    val sc = new SparkContext(new SparkConf().setAppName("datapreprocess fp"))
    val sqlContext = new HiveContext(sc)
    // 读取数据
    val data = sqlContext.sql("select * from " + inputTable)
    // 汇总用户访问的网址
    val data_fp = getFPData(data, uidCol, pidCol, newCol)
    // 存储到HDFS
    saveHiveTable(sqlContext, data_fp, output, true)
  }
}
