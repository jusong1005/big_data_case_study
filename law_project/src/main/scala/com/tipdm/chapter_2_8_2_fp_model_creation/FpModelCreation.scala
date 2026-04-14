package com.tipdm.chapter_2_8_2_fp_model_creation

import java.util.Properties

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.mllib.fpm.{AssociationRules, FPGrowth}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * 根据参数，构建模型
  * //@Author: fansy 
  * //@Time: 2019/1/14 13:54
  * //@Email: fansy1990@foxmail.com
  */
object FpModelCreation {
  val antecedent = "antecedent"
  val consequent = "consequent"
  val confidence = "confidence"
  val ts = "ts"

  def main(args: Array[String]): Unit = {
    if (args.length != 9) {
      printUsage()
      System.exit(1)
    }
    //  0. 参数处理
    val (inputTable, modelPath,modelTable, transactionColName, url, table, user, password, appName) = handle_args(args)

    //  1. 初始化
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    // 加载模型参数
    val (minSupport, minConfidence) = getModelArgs(sqlContext, url, table, user, password)

    // 2. 加载数据
    val data = sqlContext.read.table(inputTable).select(transactionColName)
      .rdd.map(row => row.getSeq[String](0)).map(x => x.toArray)
    data.cache()

    // 3. 建立模型
    val fpg = new FPGrowth().setMinSupport(minSupport).setNumPartitions(4)
    val model = fpg.run(data)

    // 4. 存储模型
    save_model_json_hive(sc,sqlContext,model.generateAssociationRules(minConfidence), modelPath,modelTable)

    // 4. 关闭SparkContext
    sc.stop()

  }

  /**
    * 保存模型
    * @param sc
    * @param sqlContext
    * @param rules
    * @param modelPath
    * @param modelTable
    */
  def save_model_json_hive(sc: SparkContext, sqlContext: HiveContext, rules: RDD[Rule[String]], modelPath:String,modelTable:String) = {
    val schema = StructType(List(
      StructField(antecedent, ArrayType(StringType), nullable = true),
      StructField(consequent, ArrayType(StringType), nullable = true),
      StructField(confidence, DoubleType, nullable = true)
    ))

    val result=rules.filter(x=>x.antecedent.length>0 && x.consequent.length>0).map(x=>Row(x.antecedent,x.consequent,x.confidence))
    val df = sqlContext.createDataFrame(result,schema)

    // save to hive
    df.write.mode(SaveMode.Overwrite).saveAsTable(modelTable)

    // save to JSON
    FileSystem.get(sc.hadoopConfiguration).delete(new Path(modelPath),true)
    df.orderBy(size(col(antecedent)).desc,col(confidence).desc).repartition(1).write.json(modelPath)
  }

  /**
    * 获得最新模型参数
    * @param sqlContext
    * @param url
    * @param table
    * @param user
    * @param password
    * @return
    */
  def getModelArgs(sqlContext:HiveContext, url: String, table: String, user: String, password: String) = {
    val properties = new Properties()
    properties.setProperty("user",user)
    properties.setProperty("password",password)
    val args = sqlContext.read.jdbc(url,table,properties ) // should include "ts", "minSupport", "minConfidence"
    val minSupport_Confidence = args.orderBy(desc(ts)).take(1).apply(0) // set "ts" as a constant
    (minSupport_Confidence.getDouble(1), minSupport_Confidence.getDouble(2))
  }

  /**
    * 处理参数
    *
    * @param args 输入参数
    * @return
    */
  def handle_args(args: Array[String]) = {
    val inputTable = args(0) // 输入数据
    val modelPath = args(1) // 输出模型路径
    val modelTable = args(2) // 输出模型Hive表
    val transactionColName = args(3) // 输入数据列名
    val url = args(4) // MySQL url连接
    val table = args(5) // MySQL 建模参数存储表
    val user = args(6) // MySQL 连接用户名
    val password = args(7) // MySQL 连接密码
    val appName = args(8) // 任务名
    (inputTable, modelPath,modelTable, transactionColName, url, table, user, password, appName)
  }

  /**
    * 使用说明
    */
  def printUsage(): Unit = {
    val buff = new StringBuilder
    buff.append("Usage : com.tipdm.chapter_2_8_2_fp_model_creation.FpModelCreation").append(" ")
      .append("<inputTable>").append(" ")
      .append("<modelPath>").append(" ")
      .append("<modelTable>").append(" ")
      .append("<transactionColName>").append(" ")
      .append("<url>").append(" ")
      .append("<table>").append(" ")
      .append("<user>").append(" ")
      .append("<password>").append(" ")
      .append("<appName>").append(" ")
    println(buff.toString())
  }
}
