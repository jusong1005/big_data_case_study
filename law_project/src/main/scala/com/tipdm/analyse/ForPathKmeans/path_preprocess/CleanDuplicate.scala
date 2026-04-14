package com.tipdm.analyse.ForPathKmeans.path_preprocess

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame

/**
  * suling
  * 在同一条轨迹中出现的相同网址记录视为一条，对用户ID跟网址记录相同的用户记录进行去重操作
  */
object CleanDuplicate {
  val sqlContext = getSparkSession("clean duplication at sanme userid and fullurl")

  def toDuplicateData(data: DataFrame) = {
    data.orderBy(timestamp_format).dropDuplicates(Array(userid, fullurl))
  }
}
