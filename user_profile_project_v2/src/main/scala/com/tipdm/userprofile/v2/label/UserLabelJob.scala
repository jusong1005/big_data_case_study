package com.tipdm.userprofile.v2.label

import com.tipdm.userprofile.v2.common.{AppConfig, JdbcSupport, SourceTables, SparkSessionFactory}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

object UserLabelJob {
  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    val spark = SparkSessionFactory.create("user-profile-v2-label", config)
    spark.sparkContext.setLogLevel("WARN")

    try {
      val tables = SourceTables.loadProcessed(spark, config)
      val retentionLabels = spark.read.parquet(config.predictionPath)
        .select(
          col("phone_no").cast("long"),
          when(col("label") === lit(1.0), lit("挽留用户")).otherwise(lit("非挽留用户")).as("label"),
          lit("用户是否挽留").as("parent_label")
        )

      val allLabels = Seq(
        consumeContentLabels(tables.billEvents),
        tvConsumeLevelLabels(tables.billEvents),
        broadbandConsumeLevelLabels(tables.billEvents),
        broadbandProductLabels(tables.orderIndex),
        offerNameLabels(tables.orderIndex),
        businessBrandLabels(tables.userMsg),
        tvNetworkAgeLabels(tables.userMsg),
        broadbandNetworkAgeLabels(tables.userMsg),
        retentionLabels
      ).reduce(_ unionByName _)
        .filter(col("phone_no").isNotNull && col("label").isNotNull && length(trim(col("label"))) > 0)
        .dropDuplicates("phone_no", "label", "parent_label")
        .cache()

      allLabels.write.mode(SaveMode.Overwrite).parquet(config.allLabelsPath)
      JdbcSupport.writeTable(allLabels, config, config.userLabelTable, SaveMode.Overwrite)

      println(s"user_label rows=${allLabels.count()} path=${config.allLabelsPath} table=${config.userLabelTable}")
    } finally {
      spark.stop()
    }
  }

  private def consumeContentLabels(billEvents: DataFrame): DataFrame = {
    billEvents.select(
      col("phone_no"),
      when(col("fee_code").isin("0J", "0B", "0Y"), lit("直播"))
        .when(col("fee_code") === lit("0X"), lit("应用"))
        .when(col("fee_code") === lit("0T"), lit("付费频道"))
        .when(col("fee_code").isin("0W", "0L", "0Z", "0K"), lit("宽带"))
        .when(col("fee_code") === lit("0D"), lit("点播"))
        .when(col("fee_code") === lit("0H"), lit("回看"))
        .when(col("fee_code") === lit("0U"), lit("有线电视收视费"))
        .as("label"),
      lit("消费内容").as("parent_label")
    ).filter(col("label").isNotNull)
  }

  private def tvConsumeLevelLabels(billEvents: DataFrame): DataFrame = {
    monthlyFee(billEvents.filter(col("sm_name").contains("电视")))
      .select(
        col("phone_no"),
        when(col("fee_per_month") > -26.5 && col("fee_per_month") < 26.5, lit("电视超低消费"))
          .when(col("fee_per_month") >= 26.5 && col("fee_per_month") < 46.5, lit("电视低消费"))
          .when(col("fee_per_month") >= 46.5 && col("fee_per_month") < 66.5, lit("电视中等消费"))
          .when(col("fee_per_month") >= 66.5, lit("电视高消费"))
          .as("label"),
        lit("电视消费水平").as("parent_label")
      ).filter(col("label").isNotNull)
  }

  private def broadbandConsumeLevelLabels(billEvents: DataFrame): DataFrame = {
    monthlyFee(billEvents.filter(col("sm_name") === lit("珠江宽频")))
      .select(
        col("phone_no"),
        when(col("fee_per_month") <= 25.0, lit("宽带低消费"))
          .when(col("fee_per_month") > 25.0 && col("fee_per_month") <= 45.0, lit("宽带中消费"))
          .when(col("fee_per_month") > 45.0, lit("宽带高消费"))
          .as("label"),
        lit("宽带消费水平").as("parent_label")
      ).filter(col("label").isNotNull)
  }

  private def broadbandProductLabels(orderIndex: DataFrame): DataFrame = {
    val latestWindow = Window.partitionBy("phone_no").orderBy(col("optdate").desc_nulls_last)
    orderIndex
      .filter(col("sm_name") === lit("珠江宽频"))
      .filter(col("effdate").isNull || col("effdate") < current_timestamp())
      .filter(col("expdate").isNull || current_timestamp() < col("expdate"))
      .withColumn("rank", row_number().over(latestWindow))
      .filter(col("rank") === 1)
      .select(col("phone_no"), col("prodname").as("label"), lit("宽带产品带宽").as("parent_label"))
  }

  private def offerNameLabels(orderIndex: DataFrame): DataFrame = {
    val latestWindow = Window.partitionBy("phone_no").orderBy(col("optdate").desc_nulls_last)
    val paid = orderIndex
      .filter(coalesce(col("cost"), lit(0.0)) > 0.0)
      .filter(!col("offername").contains("空包"))
      .filter(col("prodstatus") === lit("YY"))
      .filter(col("effdate").isNull || col("effdate") < current_timestamp())
      .filter(col("expdate").isNull || current_timestamp() < col("expdate"))

    val tvMain = paid
      .filter(col("sm_name").contains("电视"))
      .filter(col("mode_time") === lit("Y") && col("offertype") === 0)
      .withColumn("rank", row_number().over(latestWindow))
      .filter(col("rank") === 1)

    val tvSub = paid
      .filter(col("sm_name").contains("电视"))
      .filter(col("mode_time") === lit("Y") && col("offertype") === 1)

    val broadband = paid
      .filter(col("sm_name").contains("珠江宽频"))
      .withColumn("rank", row_number().over(latestWindow))
      .filter(col("rank") === 1)

    val tvMainLabels = tvMain.select(col("phone_no"), col("offername").as("label"), lit("销售品名称").as("parent_label"))
    val tvSubLabels = tvSub.select(col("phone_no"), col("offername").as("label"), lit("销售品名称").as("parent_label"))
    val broadbandLabels = broadband.select(col("phone_no"), col("offername").as("label"), lit("销售品名称").as("parent_label"))

    tvMainLabels.unionByName(tvSubLabels).unionByName(broadbandLabels)
  }

  private def businessBrandLabels(userMsg: DataFrame): DataFrame = {
    userMsg
      .filter(!col("sm_name").contains("模拟有线电视") && !col("sm_name").contains("番通"))
      .select(col("phone_no"), col("sm_name").as("label"), lit("业务品牌").as("parent_label"))
  }

  private def tvNetworkAgeLabels(userMsg: DataFrame): DataFrame = {
    networkAge(userMsg.filter(col("sm_name").contains("电视")))
      .select(
        col("phone_no"),
        when(col("years") > 8.0, lit("老用户"))
          .when(col("years") > 4.0 && col("years") <= 8.0, lit("中等用户"))
          .when(col("years") <= 4.0, lit("新用户"))
          .as("label"),
        lit("电视入网程度").as("parent_label")
      ).filter(col("label").isNotNull)
  }

  private def broadbandNetworkAgeLabels(userMsg: DataFrame): DataFrame = {
    networkAge(
      userMsg
        .filter(col("sm_name") === lit("珠江宽频"))
        .filter(col("force").contains("宽带生效"))
        .filter(col("sm_code") === lit("b0"))
    )
      .select(
        col("phone_no"),
        when(col("years") > 6.0, lit("老用户"))
          .when(col("years") > 2.0 && col("years") <= 6.0, lit("中等用户"))
          .when(col("years") <= 2.0, lit("新用户"))
          .as("label"),
        lit("宽带入网程度").as("parent_label")
      ).filter(col("label").isNotNull)
  }

  private def monthlyFee(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withColumn("real_pay", coalesce(col("should_pay"), lit(0.0)) - coalesce(col("favour_fee"), lit(0.0)))
      .groupBy("phone_no")
      .agg((sum("real_pay") / lit(3.0)).as("fee_per_month"))
  }

  private def networkAge(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .filter(col("open_time").isNotNull)
      .groupBy("phone_no")
      .agg(max(datediff(current_date(), col("open_time")) / lit(365.0)).as("years"))
  }
}
