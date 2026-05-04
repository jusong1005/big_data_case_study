package com.tipdm.userprofile.v2.common

import org.apache.spark.sql.{DataFrame, SaveMode}

object JdbcSupport {
  private val DriverClass = "com.mysql.cj.jdbc.Driver"

  def writeTable(dataFrame: DataFrame, config: AppConfig, tableName: String, saveMode: SaveMode): Unit = {
    dataFrame.write
      .format("jdbc")
      .option("url", config.mysqlUrl)
      .option("dbtable", tableName)
      .option("user", config.mysqlUser)
      .option("password", config.mysqlPassword)
      .option("driver", DriverClass)
      .mode(saveMode)
      .save()
  }
}
