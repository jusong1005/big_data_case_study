package com.tipdm.dataclean

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object DataPreprocess04_Optimization {

  /**
    * 分割为训练、测试集
    *
    * @param data
    * @param trainPercent
    * @param testPercent
    * @param sortedColumn
    * @return
    */
  def split2(data: DataFrame, trainPercent: Double, testPercent: Double, sortedColumn: String): (DataFrame, DataFrame) = {
    val (d1, d2, d3) = split3(data, trainPercent, testPercent, 0.0, sortedColumn)
    (d1, d2)
  }

  /**
    * 分割训练、验证、测试集
    *
    * @param data
    * @param trainPercent
    * @param validatePercent
    * @param testPercent
    * @param sortedColumn
    * @return
    */
  def split3(data: DataFrame, trainPercent: Double, validatePercent: Double, testPercent: Double, sortedColumn: String): (DataFrame, DataFrame, DataFrame) = {
    val percents = Array(trainPercent, validatePercent, testPercent)
    val all_size = data.count()
    val real_precents = percents.map(x => (x / percents.sum * all_size).toLong)
    //
    val allSortedColumnData = data.select(sortedColumn).orderBy(sortedColumn).rdd.map(_.getString(0)).zipWithIndex().map(x => (x._2, x._1))
    val firstSplitPoint = allSortedColumnData.lookup(real_precents(0)).head
    if (testPercent >= 0.0) {
      val secondSplitPoint = allSortedColumnData.lookup(real_precents(0) + real_precents(1)).head
      (
        data.filter(sortedColumn + " < '" + firstSplitPoint + "'"),
        data.filter(sortedColumn + " >= '" + firstSplitPoint + "' and " + sortedColumn + " < '" + secondSplitPoint + "'"),
        data.filter(sortedColumn + " >= '" + secondSplitPoint + "'")
      )
    } else {
      (
        data.filter(sortedColumn + " < " + firstSplitPoint),
        data.filter(sortedColumn + " >= " + firstSplitPoint),
        null
      )
    }
  }

  def parseArgs(args: Array[String]) = {
    // 输入表、训练集比例、验证集比例、测试集比例、训练表、验证表、测试表
    (args(0), args(1).toDouble, args(2).toDouble, args(3).toDouble, args(4), args(5), args(6))
  }

  def main(args: Array[String]): Unit = {
    val (inputTable, trainPercent, validatePercent, testPercent, trainTable, validateTable, testTable) = parseArgs(args)
    // 初始化
    val sc = new SparkContext(new SparkConf().setAppName("datapreprocss Optimization"))
    val sqlContext = new HiveContext(sc)
    // 读取原始数据
    val data = sqlContext.sql("select * from " + inputTable)
    // 数据切割，按照8:1:1的比例将数据切分成训练集、验证集、测试集
    // 分别存储在Hive表law_init1.data_101003_encoded_train，law_init1.data_101003_encoded_validate，law_init1.data_101003_encoded_test。
    val (train, validate, test) = split3(data, trainPercent, validatePercent, testPercent, timestamp_format)
    saveHiveTable(sqlContext, train, trainTable, true)
    saveHiveTable(sqlContext, validate, validateTable, true)
    saveHiveTable(sqlContext, test, testTable, true)
  }
}
