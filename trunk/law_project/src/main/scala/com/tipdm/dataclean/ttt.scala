package com.tipdm.dataclean
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.substring_index
object ttt {
  def main(args: Array[String]): Unit = {
    val sparkContext=new SparkContext(new SparkConf().setAppName("uuu"))
    val sqlContext = new HiveContext(sparkContext)
    val data = sqlContext.read.table("")
    data.withColumn("ip_two",substring_index(data("ip"), ".", 2))
  }
}
