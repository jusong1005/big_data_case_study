package com.tipdm.userprofile.v2.common

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

final case class SourceTables(
  spark: SparkSession,
  mediaIndex: DataFrame,
  userEvents: DataFrame,
  userMsg: DataFrame,
  billEvents: DataFrame,
  orderIndex: DataFrame
)

object SourceTables {
  def loadRaw(spark: SparkSession, config: AppConfig): SourceTables = {
    SourceTables(
      spark = spark,
      mediaIndex = readDelimited(spark, config.mediaIndexPath, ";", Map(
        "distinct_media_index.terminal_no" -> "terminal_no",
        "distinct_media_index.phone_no" -> "phone_no",
        "distinct_media_index.duration" -> "duration",
        "distinct_media_index.station_name" -> "station_name",
        "distinct_media_index.origin_time" -> "origin_time",
        "distinct_media_index.end_time" -> "end_time",
        "distinct_media_index.owner_code" -> "owner_code",
        "distinct_media_index.owner_name" -> "owner_name",
        "distinct_media_index.vod_cat_tags" -> "vod_cat_tags",
        "distinct_media_index.resolution" -> "resolution",
        "distinct_media_index.audio_lang" -> "audio_lang",
        "distinct_media_index.region" -> "region",
        "distinct_media_index.res_name" -> "res_name",
        "distinct_media_index.res_type" -> "res_type",
        "distinct_media_index.vod_title" -> "vod_title",
        "distinct_media_index.category_name" -> "category_name",
        "distinct_media_index.program_title" -> "program_title",
        "distinct_media_index.sm_name" -> "sm_name",
        "distinct_media_index.first_show_time" -> "first_show_time"
      ))
        .withColumn("phone_no", col("phone_no").cast("long"))
        .withColumn("duration", col("duration").cast(DoubleType))
        .withColumn("res_type", col("res_type").cast("int"))
        .withColumn("origin_time", safeTimestamp("origin_time"))
        .withColumn("end_time", safeTimestamp("end_time")),
      userEvents = readDelimited(spark, config.userEventPath, ",", Map(
        "distinct_userevent.phone_no" -> "phone_no",
        "distinct_userevent.run_name" -> "run_name",
        "distinct_userevent.run_time" -> "run_time",
        "distinct_userevent.owner_name" -> "owner_name",
        "distinct_userevent.owner_code" -> "owner_code",
        "distinct_userevent.open_time" -> "open_time",
        "distinct_userevent.sm_name" -> "sm_name"
      ))
        .withColumn("phone_no", col("phone_no").cast("long"))
        .withColumn("run_time", safeTimestamp("run_time"))
        .withColumn("open_time", safeTimestamp("open_time")),
      userMsg = readDelimited(spark, config.userMsgPath, ";", Map(
        "distinct_usermsg.terminal_no" -> "terminal_no",
        "distinct_usermsg.phone_no" -> "phone_no",
        "distinct_usermsg.sm_name" -> "sm_name",
        "distinct_usermsg.run_name" -> "run_name",
        "distinct_usermsg.sm_code" -> "sm_code",
        "distinct_usermsg.owner_name" -> "owner_name",
        "distinct_usermsg.owner_code" -> "owner_code",
        "distinct_usermsg.run_time" -> "run_time",
        "distinct_usermsg.addressoj" -> "addressoj",
        "distinct_usermsg.estate_name" -> "estate_name",
        "distinct_usermsg.open_time" -> "open_time",
        "distinct_usermsg.force" -> "force"
      ))
        .withColumn("phone_no", col("phone_no").cast("long"))
        .withColumn("run_time", safeTimestamp("run_time"))
        .withColumn("open_time", safeTimestamp("open_time")),
      billEvents = readDelimited(spark, config.billEventsPath, ";", Map(
        "distinct_billevents.terminal_no" -> "terminal_no",
        "distinct_billevents.phone_no" -> "phone_no",
        "distinct_billevents.fee_code" -> "fee_code",
        "distinct_billevents.year_month" -> "year_month",
        "distinct_billevents.owner_name" -> "owner_name",
        "distinct_billevents.owner_code" -> "owner_code",
        "distinct_billevents.sm_name" -> "sm_name",
        "distinct_billevents.should_pay" -> "should_pay",
        "distinct_billevents.favour_fee" -> "favour_fee"
      ))
        .withColumn("phone_no", col("phone_no").cast("long"))
        .withColumn("should_pay", col("should_pay").cast(DoubleType))
        .withColumn("favour_fee", col("favour_fee").cast(DoubleType)),
      orderIndex = readDelimited(spark, config.orderIndexPath, ";", Map(
        "distinct_order.phone_no" -> "phone_no",
        "distinct_order.owner_name" -> "owner_name",
        "distinct_order.optdate" -> "optdate",
        "distinct_order.prodname" -> "prodname",
        "distinct_order.sm_name" -> "sm_name",
        "distinct_order.offerid" -> "offerid",
        "distinct_order.offername" -> "offername",
        "distinct_order.business_name" -> "business_name",
        "distinct_order.owner_code" -> "owner_code",
        "distinct_order.prodprcid" -> "prodprcid",
        "distinct_order.prodprcname" -> "prodprcname",
        "distinct_order.effdate" -> "effdate",
        "distinct_order.expdate" -> "expdate",
        "distinct_order.orderdate" -> "orderdate",
        "distinct_order.cost" -> "cost",
        "distinct_order.mode_time" -> "mode_time",
        "distinct_order.prodstatus" -> "prodstatus",
        "distinct_order.run_name" -> "run_name",
        "distinct_order.orderno" -> "orderno",
        "distinct_order.offertype" -> "offertype"
      ))
        .withColumn("phone_no", col("phone_no").cast("long"))
        .withColumn("optdate", safeTimestamp("optdate"))
        .withColumn("effdate", safeTimestamp("effdate"))
        .withColumn("expdate", safeTimestamp("expdate"))
        .withColumn("orderdate", safeTimestamp("orderdate"))
        .withColumn("cost", col("cost").cast(DoubleType))
        .withColumn("offertype", col("offertype").cast("int"))
    )
  }

  def loadProcessed(spark: SparkSession, config: AppConfig): SourceTables = {
    SourceTables(
      spark = spark,
      mediaIndex = spark.read.parquet(config.processedMediaIndexPath),
      userEvents = spark.read.parquet(config.processedUserEventPath),
      userMsg = spark.read.parquet(config.processedUserMsgPath),
      billEvents = spark.read.parquet(config.processedBillEventsPath),
      orderIndex = spark.read.parquet(config.processedOrderIndexPath)
    )
  }

  private def readDelimited(spark: SparkSession, path: String, delimiter: String, renameMap: Map[String, String]): DataFrame = {
    val raw = spark.read
      .option("header", "true")
      .option("delimiter", delimiter)
      .option("multiLine", "false")
      .option("quote", "\u0000")
      .csv(path)

    renameMap.foldLeft(raw) { case (dataFrame, (source, target)) =>
      if (dataFrame.columns.contains(source)) dataFrame.withColumnRenamed(source, target) else dataFrame
    }
  }

  private def safeTimestamp(columnName: String): Column = {
    val parsed = to_timestamp(col(columnName))
    when(parsed < to_timestamp(lit("1900-01-01 00:00:00")), lit(null).cast(TimestampType)).otherwise(parsed)
  }
}
