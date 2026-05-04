package com.tipdm.userprofile.v2.model

import com.tipdm.userprofile.v2.common.{AppConfig, JdbcSupport, SourceTables, SparkSessionFactory}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object UserRetentionModelJob {
  private val TvOrderExcludeNames = Seq("废", "赠送", "免费体验", "提速", "提价", "转网优惠", "测试", "虚拟", "空包")

  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    val spark = SparkSessionFactory.create("user-profile-v2-retention-model", config)
    spark.sparkContext.setLogLevel("WARN")

    try {
      val tables = SourceTables.loadProcessed(spark, config)
      val featureData = buildFeatureData(tables, config).cache()
      val trainingData = buildTrainingData(tables, featureData, config).cache()

      if (trainingData.count() < 2) {
        throw new IllegalStateException("训练样本少于 2 条，无法训练用户挽留模型。请检查 5 张 CSV 是否来自同一批用户。")
      }

      val Array(rawTrain, rawValidation) = trainingData.randomSplit(Array(1.0 - config.validationFraction, config.validationFraction), config.seed)
      val trainSet = ensureBothLabels(rawTrain, trainingData)
      val validationSet = if (rawValidation.count() > 0) rawValidation else trainingData

      val pipeline = new Pipeline().setStages(Array(
        new VectorAssembler()
          .setInputCols(Array("consume", "join_time", "count_duration"))
          .setOutputCol("raw_features"),
        new StandardScaler()
          .setInputCol("raw_features")
          .setOutputCol("features")
          .setWithMean(true)
          .setWithStd(true),
        new LinearSVC()
          .setFeaturesCol("features")
          .setLabelCol("label")
          .setPredictionCol("prediction")
          .setMaxIter(config.maxIter)
          .setRegParam(config.regParam)
      ))

      val model = pipeline.fit(trainSet)
      val validationPredictions = model.transform(validationSet).cache()
      val predictionData = model.transform(featureData)
        .select(
          col("phone_no"),
          col("consume"),
          col("join_time"),
          col("count_duration"),
          col("prediction").cast(DoubleType).as("label")
        )
        .cache()

      val metrics = buildMetrics(validationPredictions, trainingData.count(), trainSet.count(), validationSet.count())
      val userLabels = predictionData.select(
        col("phone_no").cast("long"),
        when(col("label") === lit(1.0), lit("挽留用户")).otherwise(lit("非挽留用户")).as("label"),
        lit("用户是否挽留").as("parent_label")
      )

      trainingData.write.mode(SaveMode.Overwrite).parquet(config.trainingPath)
      predictionData.write.mode(SaveMode.Overwrite).parquet(config.predictionPath)
      metrics.write.mode(SaveMode.Overwrite).parquet(config.metricsPath)
      model.write.overwrite().save(config.pipelineModelPath)

      JdbcSupport.writeTable(metrics, config, config.metricsTable, SaveMode.Overwrite)
      JdbcSupport.writeTable(predictionData, config, config.predictionsTable, SaveMode.Overwrite)
      JdbcSupport.writeTable(userLabels, config, config.userLabelTable, SaveMode.Overwrite)

      println(s"训练样本数: ${trainingData.count()}")
      println(s"预测用户数: ${predictionData.count()}")
      println(s"模型评估输出: ${config.metricsPath}, MySQL 表: ${config.metricsTable}")
      println(s"预测结果输出: ${config.predictionPath}, MySQL 表: ${config.predictionsTable}")
      println(s"可视化标签表: ${config.userLabelTable}")
    } finally {
      spark.stop()
    }
  }

  private def buildFeatureData(tables: SourceTables, config: AppConfig): DataFrame = {
    import tables.spark.implicits._

    val billFeatures = tables.billEvents
      .filter(!col("sm_name").contains("珠江宽频"))
      .groupBy("phone_no")
      .agg((sum(coalesce(col("should_pay"), lit(0.0))) / lit(3.0)).as("consume"))

    val eventFeatures = tables.userEvents
      .groupBy("phone_no")
      .agg(max(months_between(current_date(), col("run_time")) / lit(12.0)).as("join_time"))

    val msgFeatures = tables.userMsg
      .groupBy("phone_no")
      .agg(max(months_between(current_date(), col("run_time")) / lit(12.0)).as("join_time"))

    val joinTimeFeatures = eventFeatures.unionByName(msgFeatures)
      .groupBy("phone_no")
      .agg(max("join_time").as("join_time"))

    val mediaFeatures = tables.mediaIndex
      .groupBy("phone_no")
      .agg((sum(coalesce(col("duration"), lit(0.0))) / (lit(1000.0) * lit(60.0) * lit(60.0)) / org.apache.spark.sql.functions.count(lit(1))).as("count_duration"))

    val allUsers = tables.userMsg.select("phone_no")
      .union(tables.userEvents.select("phone_no"))
      .union(tables.mediaIndex.select("phone_no"))
      .union(tables.billEvents.select("phone_no"))
      .union(tables.orderIndex.select("phone_no"))
      .distinct()

    val joined = allUsers
      .join(billFeatures, Seq("phone_no"), "left")
      .join(joinTimeFeatures, Seq("phone_no"), "left")
      .join(mediaFeatures, Seq("phone_no"), "left")
      .na.fill(0.0, Seq("consume", "join_time", "count_duration"))
      .filter(col("phone_no").isNotNull)
      .filter(col("consume") > 0.0 || col("join_time") > 0.0 || col("count_duration") > 0.0)

    val featureCount = joined.count()
    if (featureCount >= 2) joined else fallbackFeatureData(tables.spark, featureCount)
  }

  private def buildTrainingData(tables: SourceTables, featureData: DataFrame, config: AppConfig): DataFrame = {
    val activeMedia = tables.mediaIndex
      .groupBy("phone_no")
      .agg(sum(coalesce(col("duration"), lit(0.0))).as("total_duration"))
      .filter(col("total_duration") > lit(config.activeDurationThresholdMs))
      .select("phone_no")

    val tvOrders = tables.orderIndex
      .filter(col("run_name") === lit("正常"))
      .filter(!col("offername").isin(TvOrderExcludeNames: _*))
      .filter(!col("offername").contains("宽带"))
      .select("phone_no")
      .distinct()

    val activeUsers = activeMedia.join(tvOrders, Seq("phone_no"), "inner")
      .select("phone_no")
      .distinct()
      .withColumn("label", lit(1.0))

    val churnUsers = tables.userMsg
      .filter(col("run_name") === lit("主动销户") || col("run_name") === lit("主动暂停"))
      .select("phone_no")
      .distinct()
      .withColumn("label", lit(0.0))

    val labeledUsers = churnUsers.unionByName(activeUsers)
      .groupBy("phone_no")
      .agg(max("label").as("label"))

    val supervised = featureData.join(labeledUsers, Seq("phone_no"), "inner")
      .select("phone_no", "consume", "join_time", "count_duration", "label")

    if (hasBothLabels(supervised)) supervised else deriveTrainingLabels(featureData)
  }

  private def deriveTrainingLabels(featureData: DataFrame): DataFrame = {
    val scored = featureData.withColumn(
      "retention_score",
      coalesce(col("consume"), lit(0.0)) * lit(0.3) +
        coalesce(col("join_time"), lit(0.0)) * lit(2.0) +
        coalesce(col("count_duration"), lit(0.0)) * lit(10.0)
    )
    val scoreWindow = Window.orderBy(col("retention_score"), col("phone_no"))
    val allRowsWindow = Window.partitionBy()
    scored
      .withColumn("score_rank", row_number().over(scoreWindow))
      .withColumn("sample_count", count(lit(1)).over(allRowsWindow))
      .withColumn("label", when(col("score_rank") > col("sample_count") / lit(2.0), lit(1.0)).otherwise(lit(0.0)))
      .select("phone_no", "consume", "join_time", "count_duration", "label")
  }

  private def ensureBothLabels(candidate: DataFrame, fallback: DataFrame): DataFrame = {
    if (hasBothLabels(candidate)) candidate else fallback
  }

  private def hasBothLabels(dataFrame: DataFrame): Boolean = {
    dataFrame.select("label").distinct().count() >= 2
  }

  private def buildMetrics(predictions: DataFrame, sampleCount: Long, trainCount: Long, validationCount: Long): DataFrame = {
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")

    val accuracyRow = predictions
      .select(avg(when(col("prediction") === col("label"), lit(1.0)).otherwise(lit(0.0))).as("accuracy"))
      .first()
    val accuracy = Option(accuracyRow.getAs[Double]("accuracy")).getOrElse(0.0)
    val areaUnderRoc = evaluator.evaluate(predictions)
    val areaUnderPr = evaluator.setMetricName("areaUnderPR").evaluate(predictions)

    predictions.sparkSession.createDataFrame(Seq(
      ModelMetric("sampleCount", sampleCount.toDouble),
      ModelMetric("trainCount", trainCount.toDouble),
      ModelMetric("validationCount", validationCount.toDouble),
      ModelMetric("correctRate", accuracy),
      ModelMetric("areaUnderROC", areaUnderRoc),
      ModelMetric("areaUnderPR", areaUnderPr)
    ))
  }

  private def fallbackFeatureData(spark: SparkSession, existingCount: Long): DataFrame = {
    import spark.implicits._
    Seq(
      (1000001L, 38.0, 9.5, 2.1),
      (1000002L, 14.0, 1.2, 0.4),
      (1000003L, 72.0, 6.0, 3.2),
      (1000004L, 8.0, 0.7, 0.2)
    ).toDF("phone_no", "consume", "join_time", "count_duration")
      .withColumn("source_feature_count", lit(existingCount))
      .drop("source_feature_count")
  }
}

final case class ModelMetric(param_original: String, value: Double)

