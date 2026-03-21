package com.tipdm.scala.processing
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
/**
  * Created by ch on 2019/1/23
  */
object SVMDataProcess {
  def main(args: Array[String]): Unit = {
    if (args.length != 7) {
      printUsage()
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("SVMDataProcess")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    //账单表
    val billevent = args(0)
    //用户状态信息变更表
    val mediamatchUserevents = args(1)
    //用户收视行为信息表
    val tmpMediaIndex = args(2)
    //用户基本信息表
    val mediamatchUsermsg = args(3)
    //订单表
    val order_index = args(4)
    // 训练表
    val trainTable = args(5)
    // 待预测表
    val toBePredictedTable = args(6)
    // 电视用户活跃度标签计算
    val msg = sqlContext.sql("select distinct phone_no,0 as col1 from " + mediamatchUsermsg)
//    val mediaIndex = sqlContext.sql("select phone_no,sum(duration) as total_one_month_seconds from " + tmpMediaIndex +
//      " group by phone_no having total_one_month_seconds>18720000").select("phone_no", "total_one_month_seconds")
    val originMedia = sqlContext.sql("select * from "+tmpMediaIndex)
    val mediaIndex = originMedia.groupBy("phone_no").agg(sum("duration").alias("total_one_month_seconds")).filter("total_one_month_seconds>18720000")

    val orderIndexTV = sqlContext.sql("select * from " + order_index + " where run_name='正常' and offername!='废' and " +
      "offername!='赠送' and offername!='免费体验' and offername!='提速' and offername!='提价' and offername!='转网优惠' and offername!='测试' and offername!='虚拟' and offername!='空包' and offername not like '%宽带%'").select("phone_no").distinct()

    val activeJoin2 = mediaIndex.join(orderIndexTV, Seq("phone_no"), "inner").selectExpr("phone_no", "1 as col2").distinct()
    // 用户活跃度，col1为1表示为活跃用户，0表示为不活跃用户
    val activeJoin3 = msg.join(activeJoin2, Seq("phone_no"), "left_outer").na.fill(0).selectExpr("phone_no", "col2 as col1")
    // 构造svm指标
    // 统计每个用户的月均消费金额C
    val billevents = sqlContext.sql("select phone_no, sum(should_pay)/3 consume  from " + billevent + " where sm_name not like '%珠江宽频%' group by phone_no")
    // 统计每个用户的入网时长max(当前时间-run_time)
    val userevents = sqlContext.sql("select phone_no,max(months_between(current_date(),run_time)/12) join_time from " + mediamatchUserevents + " group by phone_no")
    // 统计每个用户平均每次看多少小时电视M
    val media_index = sqlContext.sql("select phone_no,(sum(media.duration)/(1000*60*60))/count(1) as count_duration from " + tmpMediaIndex + " media group by phone_no")
    val join1 = billevents.join(userevents, Seq("phone_no")).join(media_index, Seq("phone_no"))
    // mediamatch_usermsg选出离网的用户（run_name ='主动销户' or run_name='主动暂停' )贴上类别0（离网）；在正常用户中提取有活跃标签的用户贴上类别1（不离网）。
    val usermsg = sqlContext.sql("select *  from " + mediamatchUsermsg + " where  run_name ='主动销户' or run_name='主动暂停' ")
    // 给离网用户贴上类别0
    val join2 = usermsg.join(join1, Seq("phone_no"), "inner").withColumn("label", join1("consume") * 0)
    // 在正常用户中提取有活跃标签的用户贴上类别1（不离网）
    val activateUser = activeJoin3.where("col1=1")
    val join3 = join1.join(activateUser, Seq("phone_no"), "inner").withColumn("label", join1("consume") * 0 + 1)
    val unionData = join2.select("phone_no", "consume", "join_time", "count_duration", "label").unionAll(join3.select("phone_no", "consume", "join_time", "count_duration", "label"))
    unionData.write.mode(SaveMode.Overwrite).saveAsTable(trainTable)
    join1.write.mode(SaveMode.Overwrite).saveAsTable(toBePredictedTable)
    sc.stop()
  }
  /**
    * 使用说明
    */
  def printUsage(): Unit = {
    val buff = new StringBuilder
    buff.append("Usage : com.tipdm.scala.processing.SVMDataProcess").append(" ")
      .append("<billeventTable>").append(" ")
      .append("<mediamatchUsereventsTable>").append(" ")
      .append("<mediaIndexTable>").append(" ")
      .append("<mediamatchUsermsgTable>").append(" ")
      .append("<orderIndexTable>").append(" ")
      .append("<trainTable>").append(" ")
      .append("<toBePredictedTable>").append(" ")
    println(buff.toString())
  }

}
