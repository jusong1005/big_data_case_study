package com.tipdm.userprofile.v2.etl

import com.tipdm.userprofile.v2.common.{AppConfig, SourceTables, SparkSessionFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

object OfflineEtlJob {
  private val EnterpriseLevels = Seq("EA级", "EB级", "EC级", "ED级", "EE级")
  private val SpecialOwnerCodes = Seq("02", "09", "10")
  private val ValidServiceNames = Seq("珠江宽频", "数字电视", "互动电视", "甜果电视")
  private val ValidRunNames = Seq("正常", "主动暂停", "欠费暂停", "主动销户")

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    val spark = SparkSessionFactory.create("user-profile-v2-offline-etl", config)
    spark.sparkContext.setLogLevel("WARN")

    try {
      val rawTables = SourceTables.loadRaw(spark, config)
      val processedTables = SourceTables(
        spark = spark,
        mediaIndex = processMedia(rawTables.mediaIndex),
        userEvents = processUserEvent(rawTables.userEvents),
        userMsg = processUserMsg(rawTables.userMsg),
        billEvents = processCommon(rawTables.billEvents),
        orderIndex = processOrder(rawTables.orderIndex)
      )

      write(processedTables.mediaIndex, config.processedMediaIndexPath, "media_index_process")
      write(processedTables.userEvents, config.processedUserEventPath, "mediamatch_userevent_process")
      write(processedTables.userMsg, config.processedUserMsgPath, "mediamatch_usermsg_process")
      write(processedTables.billEvents, config.processedBillEventsPath, "mmconsume_billevents_process")
      write(processedTables.orderIndex, config.processedOrderIndexPath, "order_index_process")
    } finally {
      spark.stop()
    }
  }

  private def processMedia(dataFrame: DataFrame): DataFrame = {
    processCommon(dataFrame)
      .filter(col("duration") >= 20000.0 && col("duration") <= 18000000.0)
      .filter(col("phone_no") =!= 5401487L)
      .filter(
        col("res_type") =!= 0 ||
          !date_format(col("origin_time"), "ss").rlike("00$") ||
          !date_format(col("end_time"), "ss").rlike("00$")
      )
  }

  private def processUserMsg(dataFrame: DataFrame): DataFrame = {
    val common = processCommon(dataFrame)
    val latestTime = common.groupBy("phone_no").agg(max("run_time").as("run_time"))
    common.join(latestTime, Seq("phone_no", "run_time"), "inner")
      .filter(col("run_name").isin(ValidRunNames: _*))
  }

  private def processUserEvent(dataFrame: DataFrame): DataFrame = {
    processCommon(dataFrame).filter(col("run_name").isin(ValidRunNames: _*))
  }

  private def processOrder(dataFrame: DataFrame): DataFrame = {
    processCommon(dataFrame).filter(col("run_name").isin(ValidRunNames: _*))
  }

  private def processCommon(dataFrame: DataFrame): DataFrame = {
    dataFrame.distinct()
      .filter(!col("owner_name").isin(EnterpriseLevels: _*))
      .filter(!col("owner_code").isin(SpecialOwnerCodes: _*))
      .filter(col("sm_name").isin(ValidServiceNames: _*))
      .filter(col("phone_no").isNotNull)
  }

  private def write(dataFrame: DataFrame, path: String, name: String): Unit = {
    val rows = dataFrame.count()
    dataFrame.write.mode(SaveMode.Overwrite).parquet(path)
    println(s"$name rows=$rows path=$path")
  }
}
