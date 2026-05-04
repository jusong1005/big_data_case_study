package com.tipdm.userprofile.v2.common

import java.io.{FileInputStream, InputStream}
import java.util.Properties

final case class AppConfig(
  sparkMaster: String,
  sparkWarehouseDir: String,
  mysqlUrl: String,
  mysqlUser: String,
  mysqlPassword: String,
  metricsTable: String,
  predictionsTable: String,
  userLabelTable: String,
  dataRoot: String,
  rawPath: String,
  processedPath: String,
  outputPath: String,
  activeDurationThresholdMs: Double,
  maxIter: Int,
  regParam: Double,
  validationFraction: Double,
  seed: Long
) {
  val mediaIndexPath: String = s"$rawPath/media_index_test.csv"
  val userEventPath: String = s"$rawPath/mediamatch_userevent.csv"
  val userMsgPath: String = s"$rawPath/mediamatch_usermsg.csv"
  val billEventsPath: String = s"$rawPath/mmconsume_billevents.csv"
  val orderIndexPath: String = s"$rawPath/order_index_test.csv"
  val processedMediaIndexPath: String = s"$processedPath/media_index_process"
  val processedUserEventPath: String = s"$processedPath/mediamatch_userevent_process"
  val processedUserMsgPath: String = s"$processedPath/mediamatch_usermsg_process"
  val processedBillEventsPath: String = s"$processedPath/mmconsume_billevents_process"
  val processedOrderIndexPath: String = s"$processedPath/order_index_process"
  val allLabelsPath: String = s"$outputPath/user_labels"
  val trainingPath: String = s"$outputPath/training_features"
  val predictionPath: String = s"$outputPath/predictions"
  val metricsPath: String = s"$outputPath/metrics"
  val pipelineModelPath: String = s"$outputPath/pipeline_model"
}

object AppConfig {
  private val DefaultResource = "application-dev.properties"

  def load(): AppConfig = {
    val properties = new Properties()
    loadResource(properties, DefaultResource)
    Option(System.getProperty("app.config")).foreach(loadFile(properties, _))

    AppConfig(
      sparkMaster = read(properties, "spark.master", "local[*]"),
      sparkWarehouseDir = read(properties, "spark.warehouse.dir", "file:///home/hadoop/export/data/user_profile_project_v2/warehouse"),
      mysqlUrl = read(properties, "mysql.url", "jdbc:mysql://localhost:3306/big_data_case_study?useSSL=false&serverTimezone=UTC&characterEncoding=utf8&allowPublicKeyRetrieval=true"),
      mysqlUser = read(properties, "mysql.user", "bigdata"),
      mysqlPassword = read(properties, "mysql.password", "BigData@123456"),
      metricsTable = read(properties, "mysql.table.metrics", "user_profile_svm_metrics"),
      predictionsTable = read(properties, "mysql.table.predictions", "user_profile_svm_prediction"),
      userLabelTable = read(properties, "mysql.table.user-label", "user_label"),
      dataRoot = read(properties, "data.root", "/home/hadoop/export/data/user_profile_project_v2"),
      rawPath = read(properties, "data.raw.path", "/home/hadoop/export/data/user_profile_project_v2/raw/media_index_sample"),
      processedPath = read(properties, "data.processed.path", "/home/hadoop/export/data/user_profile_project_v2/processed"),
      outputPath = read(properties, "data.output.path", "/home/hadoop/export/data/user_profile_project_v2/model"),
      activeDurationThresholdMs = read(properties, "model.active-duration-threshold-ms", "18720000").toDouble,
      maxIter = read(properties, "model.max-iter", "60").toInt,
      regParam = read(properties, "model.reg-param", "0.01").toDouble,
      validationFraction = read(properties, "model.validation-fraction", "0.2").toDouble,
      seed = read(properties, "model.seed", "20260504").toLong
    )
  }

  private def loadResource(properties: Properties, resourceName: String): Unit = {
    val stream = Option(Thread.currentThread().getContextClassLoader.getResourceAsStream(resourceName))
    stream.foreach(load(properties, _))
  }

  private def loadFile(properties: Properties, path: String): Unit = {
    load(properties, new FileInputStream(path))
  }

  private def load(properties: Properties, inputStream: InputStream): Unit = {
    try properties.load(inputStream)
    finally inputStream.close()
  }

  private def read(properties: Properties, key: String, defaultValue: String): String = {
    Option(System.getProperty(key))
      .orElse(Option(System.getenv(key.toUpperCase.replace('.', '_').replace('-', '_'))))
      .orElse(Option(properties.getProperty(key)))
      .getOrElse(defaultValue)
  }
}
