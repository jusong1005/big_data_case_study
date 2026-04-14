package com.tipdm.recommendsave

import com.tipdm.util.CommonUtil._
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RecommendSave {


  def saveData(jobConf: JobConf, column_family: String, column: String, rdd: RDD[(String, Array[String])]) = {
    val rdd_hbase = rdd.map { arr => {
      val put = new Put(Bytes.toBytes(arr._1))
      for (rec <- arr._2) {
        put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes(column), System.currentTimeMillis(), Bytes.toBytes(rec))
      }
      (new ImmutableBytesWritable(Bytes.toBytes(arr._1)), put)
    }
    }
    rdd_hbase.saveAsHadoopDataset(jobConf)
  }

  def parseArgs(args: Array[String]) = {
    // appName,表名，列簇名，列名，推荐结果文件
    (args(0), args(1), args(2), args(3), args(4))
  }

  def main(args: Array[String]): Unit = {
    val (appName, tableName, column_family, column, input) = parseArgs(args)
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)
    val data = sc.textFile(input).map { x => val xx = x.split("\\|\\|\\|"); (xx(0), xx.slice(1, xx.length)) }.filter(_._2.length > 0)
    val jobConf = hbaseConnect(tableName)
    saveData(jobConf, column_family, column, data)
  }
}