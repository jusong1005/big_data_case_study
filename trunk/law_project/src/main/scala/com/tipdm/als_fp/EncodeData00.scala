package com.tipdm.als_fp

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 对数据进行编码
  * //@Author: fansy 
  * //@Time: 2018/10/24 16:00
  * //@Email: fansy1990@foxmail.com
  */
object EncodeData00 {

  case class User_ID(user: String, id: Long)

  case class Item_ID(item: String, id: Long)

  /**
    * 数据编码
    *
    * @param spark
    * @param data
    * @param userCol
    * @param itemCol
    * @param userMetaTable user元数据
    * @param itemMetaTable item元数据
    * @return
    */
  def encode_data(spark: SparkContext, sqlContext: SQLContext,
                  data: DataFrame, userCol: String, itemCol: String,
                  newUserCol: String, newItemCol: String,
                  userMetaTable: String, itemMetaTable: String): DataFrame = {
    import sqlContext.implicits._
    val allUsers = data.select(userCol).distinct().orderBy(userCol).rdd.map(_.getString(0))
      .zipWithIndex().map(x => User_ID(x._1, x._2)).toDF
    val allItems = data.select(itemCol).distinct().orderBy(itemCol).rdd.map(_.getString(0))
      .zipWithIndex().map(x => Item_ID(x._1, x._2)).toDF

    // save
    allUsers.registerTempTable("userMetaTable_00")
    allItems.registerTempTable("itemMetaTable_00")

    sqlContext.sql("create table " + userMetaTable + " as select * from userMetaTable_00")
    sqlContext.sql("create table " + itemMetaTable + " as select * from itemMetaTable_00")

    // join
    data.join(allUsers, data(userCol) === allUsers("user"), "leftouter").drop(allUsers("user"))
      .withColumnRenamed("id", newUserCol).join(allItems, data(itemCol) === allItems("item"), "leftouter")
      .drop(allItems("item")).withColumnRenamed("id", newItemCol)
  }

  def getUsage(): String = {
    val buff = new StringBuilder
    buff.append("Usage: com.tipdm.als_fp.EncodeData00 ").append(" ")
      .append("<inputTable>").append(" ")
      .append("<userCol>").append(" ")
      .append("<itemCol>").append(" ")
      .append("<newUserCol>").append(" ")
      .append("<newItemCol>").append(" ")
      .append("<userMetaTable>").append(" ")
      .append("<itemMetaTable>").append(" ")
      .append("<appName>").append(" ")
    buff.toString()
  }

  def handle_args(args: Array[String]) = {
    //inputTable, userCol , itemCol , newUserCol, newItemCol, userMetaTable, itemMetaTable, appName
    (args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7))
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 8) {
      println(getUsage())
      System.exit(1)
    }

    // 0. 处理参数
    val (inputTable, userCol, itemCol, newUserCol, newItemCol, userMetaTable, itemMetaTable, appName) = handle_args(args)

    // 1. 初始化
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 2. 读取数据
    val data = sqlContext.sql("select * from " + inputTable)

    // 3. 编码
    val encoded_data = encode_data(sc, sqlContext, data, userCol, itemCol, newUserCol, newItemCol, userMetaTable, itemMetaTable)

    // 4. 存储
    encoded_data.registerTempTable("encodedata")
    sqlContext.sql("create table law_init1.data_101003_encoded as select * from encodedata")
  }
}
