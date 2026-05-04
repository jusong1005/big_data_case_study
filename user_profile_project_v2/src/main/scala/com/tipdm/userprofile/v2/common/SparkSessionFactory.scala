package com.tipdm.userprofile.v2.common

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {
  def create(appName: String, config: AppConfig): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master(config.sparkMaster)
      .config("spark.sql.warehouse.dir", config.sparkWarehouseDir)
      .config("spark.sql.session.timeZone", "Asia/Shanghai")
      .getOrCreate()
  }
}
