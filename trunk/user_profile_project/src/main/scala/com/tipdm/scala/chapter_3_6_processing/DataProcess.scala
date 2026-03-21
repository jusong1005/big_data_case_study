package com.tipdm.scala.chapter_3_6_processing

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
  * //@Author:qwm
  * //@Date: 2018/8/17 09:33
  *
  * 数据预处理
  */
object DataProcess {
  def main(args: Array[String]): Unit = {
    if (args.length != 10) {
      printUsage()
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("DataProcess")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    // media_index数据预处理
    val originMediaIndexTable = args(0)
    val processMediaIndexTable = args(1)
    dataProcessing(sqlContext, originMediaIndexTable, "media", processMediaIndexTable)
    // mediamatch_userevent数据预处理
    val originMediamatchUsereventTable = args(2)
    val processMediamatchUsereventTable = args(3)
    dataProcessing(sqlContext, originMediamatchUsereventTable, "userevent", processMediamatchUsereventTable)
    //mediamatch_usermsg数据处理
    val originalMediamatchUsermsgTable = args(4)
    val processMediamatchUsermsgTable = args(5)
    dataProcessing(sqlContext, originalMediamatchUsermsgTable, "usermsg", processMediamatchUsermsgTable)
    //mmconsume_billevents数据预处理
    val originalMMConsumeBilleventsTable = args(6)
    val processMMConsumeBilleventsTable = args(7)
    dataProcessing(sqlContext, originalMMConsumeBilleventsTable, "bill", processMMConsumeBilleventsTable)
    //order_index数据预处理
    val originalOrderIndexTable = args(8)
    val processOrderIndexTable = args(9)
    dataProcessing(sqlContext, originalOrderIndexTable, "order", processOrderIndexTable)
    sc.stop()
  }

  /**
    *
    * @param sqlContext
    * @param inputTable  Hive输入表名称
    * @param flag        各表的标记符，由于有些表的处理逻辑不同(值为：media,usermsg，userevent,order,bill
    * @param outputTable 输出到Hive表的名称
    */
  def dataProcessing(sqlContext: HiveContext, inputTable: String, flag: String, outputTable: String): Unit = {
    val df = sqlContext.sql("select * from " + inputTable)
    //数据去重，并去除政企用户
    val commonDF = df.distinct().filter("owner_name!='EA级' and owner_name!='EB级' and owner_name!='EC级' and owner_name!='ED级' and owner_name!='EE级'")
      //去除特殊线路的用户
      .filter("owner_code!='02' and owner_code!='09' and owner_code!='10'")
      //保留sm_name='珠江宽频,'数字电视' ,'互动电视' ,'甜果电视'
      .filter("sm_name='珠江宽频' or sm_name='数字电视' or sm_name='互动电视' or sm_name='甜果电视'")
    val resultDF = if (flag.equals("media")) {
      //用户收视行为记录保留duration>=20秒且<=5小时
      commonDF.filter("duration>=20000 and duration<=18000000")
        //过滤用户编号为5401487的记录
        .filter("phone_no!=5401487")
        //过滤res_type!=0 or origin_time not rlike '00$' or end_time not rlike '00$'的记录
        .filter(col("res_type").notEqual(0) or !col("origin_time").rlike("00$") or !col("end_time").rlike("00$"))
    } else if (flag.equals("usermsg")) {
      val maxTimeUsermsg = commonDF.groupBy("phone_no").agg(max("run_time").alias("run_time"))
      commonDF.join(maxTimeUsermsg, Seq("phone_no", "run_time"))
        .filter("run_name='正常' or run_name='主动暂停' or run_name='欠费暂停' or run_name='主动销户'")
    } else if (flag.endsWith("order") || flag.equals("userevent")) {
      //保留run_name='正常','主动暂停','欠费暂停','主动销户'
      commonDF.filter("run_name='正常' or run_name='主动暂停' or run_name='欠费暂停' or run_name='主动销户'")
    } else {
      commonDF
    }
    //输出到Hive表
    resultDF.write.mode(SaveMode.Overwrite).saveAsTable(outputTable)
  }

  /**
    * 使用说明
    */
  def printUsage(): Unit = {
    val buff = new StringBuilder
    buff.append("Usage : com.tipdm.scala.processing.DataProcess").append(" ")
      .append("<originMediaIndexTable>").append(" ")
      .append("<processMediaIndexTable>").append(" ")
      .append("<originMediamatchUsereventTable>").append(" ")
      .append("<processMediamatchUsereventTable>").append(" ")
      .append("<originalMediamatchUsermsgTable>").append(" ")
      .append("<processMediamatchUsermsgTable>").append(" ")
      .append("<originalMMConsumeBilleventsTable>").append(" ")
      .append("<processMMConsumeBilleventsTable>").append(" ")
      .append("<originalOrderIndexTable>").append(" ")
      .append("<processOrderIndexTable>").append(" ")
    println(buff.toString())
  }
}
