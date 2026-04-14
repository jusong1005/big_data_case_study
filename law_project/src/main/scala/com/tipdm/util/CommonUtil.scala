package com.tipdm.util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object CommonUtil {
  val userid = "userid"
  val fullurl = "fullurl"
  val timestamp = "timestamp_format"
  val timestamp_format = "timestamp_format"
  val fullurlid = "fullurlid"
  val table_gt_oneclick_data = "law_init1.lawtime_gt_one"
  val table_gt_oneclick_data_distinct = "law_init1.lawtime_gt_one_distinct"
  val lawtime_all = "law_init1.lawtime_all"
  val table_101003 = "law_init.data_101003_processed"
  val table_101003_gt_one = "law_init.data_101003_url_gt_one"
  val table_101003_gt_one_distinct = "law_init.data_101003_url_gt_one_distinct"
  val features = "features"
  val label = "label"
  val visit_count = "visit_count"
  val visit_duration = "visit_duration"
  val visit_relevant_count = "visit_relevant_count"
  val visit_relevant_count2 = "visit_relevant_count2"

  val scaled_features = "scaled_features"
  val assembled_features = "assembled_features"
  val filtered_features = "filtered_features"

  val predict_column = "predict_column"
  val fullurlWithid = "law_init.fullurl_id0"
  val modelPath = "/user/root/pathModel"
  val centerPath = "/user/root/pathCenter"
  val predict_col = "prediction"

  val indexedLabel = "indexedLabel"
  val indexedFeatures = "indexedFeatures"
  val predictedLabel = "predictedLabel"

  val data_101003_real_url_gt_one = "law_init1.data_101003_real_url_gt_one"

  def getSparkSession(appName: String) = {
    val sc = new SparkContext(new SparkConf().setAppName(appName))
    sc.setLogLevel("ERROR")
    val sqlContext = new HiveContext(sc)
    sqlContext
  }

  def getSparkSession() = {
    val sc = new SparkContext(new SparkConf().setAppName("read data"))
    sc.setLogLevel("ERROR")
    val sqlContext = new HiveContext(sc)
    sqlContext
  }

  /**
    * 保存表,直接覆盖已存在的表
    *
    * @param sqlContext
    * @param data
    * @param overwrite
    */
  def saveHiveTable(sqlContext: HiveContext, data: DataFrame, outTable: String, overwrite: Boolean): Unit = {
    if (overwrite) {
      sqlContext.sql("drop table if exists " + outTable)
    }
    val tmpTableName = "t" + System.currentTimeMillis()
    data.registerTempTable(tmpTableName)
    sqlContext.sql("create table " + outTable + " as select * from " + tmpTableName)
  }

  /**
    * 直接保存表
    *
    * @param sqlContext
    * @param data
    * @param outTable
    */
  def saveHiveTable(sqlContext: HiveContext, data: DataFrame, outTable: String): Unit = {
    saveHiveTable(sqlContext, data, outTable, false)
  }

  /**
    * 删除HDFS上的文件
    *
    * @param path
    * @param sqlContext
    * @return
    */
  def deletePath(path: Path, sqlContext: HiveContext) = {
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    if (hdfs.exists(path)) {
      hdfs.delete(path, true)
    }
  }

  def deletePath(path: String, sqlContext: HiveContext) = {
    val hadoopConf = sqlContext.sparkContext.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val path2 = new Path(path)
    if (hdfs.exists(path2)) {
      hdfs.delete(path2, true)
    }
  }

  def saveAsHDFSFile(data: RDD[String], output: String, deleteOrNot: Boolean, sqlContext: HiveContext) = {
    if (deleteOrNot == true) {
      deletePath(new Path(output), sqlContext)
      data.saveAsTextFile(output)
    }
    else {
      data.saveAsTextFile(output)
    }
  }

  def hbaseConnect(tablename: String) = {
    val conf = HBaseConfiguration.create()
    // 设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置  
    conf.set("hbase.zookeeper.quorum", "node2,node3,node4")
    // 设置zookeeper连接端口，默认2181  
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)
    jobConf
  }
}
