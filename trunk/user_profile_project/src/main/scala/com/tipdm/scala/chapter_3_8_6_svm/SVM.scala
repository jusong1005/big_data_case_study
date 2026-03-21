package com.tipdm.scala.chapter_3_8_6_svm

import com.tipdm.scala.util.SparkUtils.exists
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/*
* //@Author:qwm
* //@Date: 2018/8/17 09:33
*
* 支持向量机算法分类用户是否挽留
* */

object SVM {
  def main(args: Array[String]): Unit = {
    if (args.length != 12) {
      printUsage()
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("SVM").setJars(Seq("/opt/cloudera/parcels/CDH-5.7.3-1.cdh5.7.3.p0.5/lib/hive/lib/mysql-connector-java-5.1.7-bin.jar"))
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val mmconsume = args(0)
    val mediamatchUserevents = args(1)
    val tmpMediaIndex = args(2)
    val mediamatchUsermsg = args(3)
    val order_index = args(4)
    // 评价结果表
    val activate_table = args(5)
    // 预测表
    val predictionTable = args(6)
    val numItertions = args(7).toInt
    val stepSize = args(8).toDouble
    val regParam = args(9).toDouble
    val miniBatchFraction = args(10).toDouble
    val db = args(11)
    // 电视用户活跃度标签计算
    val msg = sqlContext.sql("select distinct phone_no,0 as col1 from " + db + "." + mediamatchUsermsg)
    val mediaIndex = sqlContext.sql("select phone_no,sum(duration) as total_one_month_seconds from " + db + "." + tmpMediaIndex +
      " where origin_time>=add_months('2018-08-01 00:00:00',-1) group by phone_no having total_one_month_seconds>18936000").select("phone_no", "total_one_month_seconds")
    val orderIndexTV = sqlContext.sql("select * from " + db + "." + order_index + " where run_name='正常' and offername!='废' and " +
      "offername!='赠送' and offername!='免费体验' and offername!='提速' and offername!='提价' and offername!='转网优惠' and offername!='测试' and offername!='虚拟' and offername!='空包' and offername not like '%宽带%'").select("phone_no").distinct()
    val media_order = mediaIndex.join(orderIndexTV, Seq("phone_no"), "inner").selectExpr("phone_no", "1 as col2").distinct()
    // 用户活跃度，col1为1表示为活跃用户，0表示为不活跃用户
    val msg_media_order = msg.join(media_order, Seq("phone_no"), "left_outer").na.fill(0).selectExpr("phone_no", "col2 as col1")
    // 构造svm数据
    // 统计每个用户的月均消费金额C
    val billevents = sqlContext.sql("select phone_no, sum(should_pay)/3 consume  from " + db + "." + mmconsume + " where sm_name not like '%珠江宽频%' group by phone_no")
    // 统计每个用户的入网时长max(当前时间-run_time)
    val userevents = sqlContext.sql("select phone_no,max(months_between(current_date(),run_time)/12) join_time from " + db + "." + mediamatchUserevents + " group by phone_no")
    // 统计每个用户平均每次看多少小时电视M
    val media_index = sqlContext.sql("select phone_no,(sum(media.duration)/(1000*60*60))/count(1) as count_duration from " + db + "." + tmpMediaIndex + " media group by phone_no")
    val billevents_userevents_media = billevents.join(userevents, Seq("phone_no")).join(media_index, Seq("phone_no"))
    // mediamatch_usermsg选出离网的用户（run_name ='主动销户' or run_name='主动暂停' )贴上类别0（离网）；在正常用户中提取有活跃标签的用户贴上类别1（不离网）。
    val usermsg = sqlContext.sql("select *  from " + db + "." + mediamatchUsermsg + " where  run_name ='主动销户' or run_name='主动暂停' ")
    // 给离网用户贴上类别0
    val usermsg_billevents_userevents = usermsg.join(billevents_userevents_media, Seq("phone_no"), "inner").withColumn("label", billevents_userevents_media("consume") * 0)
    // 在正常用户中提取有活跃标签的用户贴上类别1（不离网）
    val activateUser = msg_media_order.where("col1=1")
    val billevents_userevents_activateUser = billevents_userevents_media.join(activateUser, Seq("phone_no"), "inner").withColumn("label", billevents_userevents_media("consume") * 0 + 1)
    val unionData = usermsg_billevents_userevents.select("phone_no", "consume", "join_time", "count_duration", "label").unionAll(billevents_userevents_activateUser.select("phone_no", "consume", "join_time", "count_duration", "label"))
    // 训练数据为union_data
    val traindata = unionData.select("consume", "join_time", "count_duration").rdd.
      zip(unionData.select("label").rdd).map(x => LabeledPoint(x._2.get(0).toString.toDouble,
      Vectors.dense(x._1.toSeq.toArray.map(_.toString.toDouble))))
    // 测试数据集
    val test_data = billevents_userevents_media.select("phone_no").rdd.zip(billevents_userevents_media.select("consume", "join_time", "count_duration").rdd).
      map(x => (x._1.get(0).toString, Vectors.dense(x._2.toSeq.toArray.map(_.toString.toDouble))))
    //归一化
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(traindata.map(x => x.features))
    val data2 = traindata.map(x => LabeledPoint(x.label, scaler.transform(Vectors.dense(x.features.toArray))))
    val data2_test = data2.map(x => (x.label, scaler.transform(Vectors.dense(x.features.toArray))))
    //将数据分为训练集和验证集
    val train_validate = data2.randomSplit(Array(0.8, 0.2))
    val (train_data, validate_data) = (train_validate(0), train_validate(1))
    train_data.cache()
    validate_data.cache()
    //建模
    val model = SVMWithSGD.train(train_data, numItertions, stepSize, regParam, miniBatchFraction)
    //val model = SVMWithSGD.train(train_data,10, 1.0, 0.01,  1.0)
    train_data.unpersist()
    //评估
    val predictAndLabel = validate_data.map(row => {
      val predict = model.predict(row.features)
      val label = row.label
      (predict, label)
    })
    val right_count = predictAndLabel.filter(r => r._1 == r._2).count.toDouble
    val validate_count = validate_data.count()
    val validateCorrectRate = right_count / validate_count
    val predictAndLabelNew = predictAndLabel.repartition(100)
    val metrics = new BinaryClassificationMetrics(predictAndLabelNew)
    val schema1 = StructType(Array(
      StructField("param_original", StringType, false),
      StructField("value", DoubleType, false)))
    val rdd1 = sc.parallelize(Array(
      Row("correctRate", validateCorrectRate),
      Row("areaUnderROC", metrics.areaUnderROC()),
      Row("areaUnderPR", metrics.areaUnderPR())
    ))
    val evaluation = sqlContext.createDataFrame(rdd1, schema1)
    val tmpTable = "tmp" + System.currentTimeMillis()
    evaluation.registerTempTable(tmpTable)
    if (exists(sqlContext, db, activate_table)) {
      sqlContext.sql("drop table " + db + "." + activate_table)
      sqlContext.sql("create table " + db + "." + activate_table + " as select * from " + tmpTable)
    } else {
      sqlContext.sql("create table " + db + "." + activate_table + " as select * from " + tmpTable)
    }
    validate_data.unpersist()
    //预测
    val predictData = test_data.map(row => {
      val predict = model.predict(row._2)
      Row(row._1, row._2(0), row._2(1), row._2(2), predict)
    })
    val schema = StructType(Array(StructField("phone_no", StringType, false), StructField("consume", DoubleType, false), StructField("join_time", DoubleType, false), StructField("count_duration", DoubleType, false), StructField("label", DoubleType, false)))
    val predictDF = sqlContext.createDataFrame(predictData, schema)
    val tmpTable1 = "tmp" + System.currentTimeMillis()
    predictDF.registerTempTable(tmpTable1)
    if (exists(sqlContext, db, predictionTable)) {
      sqlContext.sql("drop table " + db + "." + predictionTable)
      sqlContext.sql("create table " + db + "." + predictionTable + " as select * from " + tmpTable1)
    } else {
      sqlContext.sql("create table " + db + "." + predictionTable + " as select * from " + tmpTable1)
    }
    sc.stop()
  }

  /**
    * 使用说明
    */
  def printUsage(): Unit = {
    val buff = new StringBuilder
    buff.append("Usage : com.tipdm.scala.charact_3_8_6_svm.SVM").append(" ")
      .append("<mmconsume>").append(" ")
      .append("<mediamatchUserevents>").append(" ")
      .append("<tmpMediaIndex>").append(" ")
      .append("<mediamatchUsermsg>").append(" ")
      .append("<order_index>").append(" ")
      .append("<activate_table>").append(" ")
      .append("<predictionTable>").append(" ")
      .append("<numItertions>").append(" ")
      .append("<stepSize>").append(" ")
      .append("<regParam>").append(" ")
      .append("<miniBatchFraction>").append(" ")
      .append("<db>").append(" ")
    println(buff.toString())
  }
}
