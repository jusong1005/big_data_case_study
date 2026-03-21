package com.tipdm.analyse.ForPathKmeans.path_preprocess

import com.tipdm.util.CommonUtil.fullurlWithid
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object URLMapID {

  def MapURLToID(data: DataFrame, sqlContext: HiveContext) = {
    val fullurl_id = data.select("fullurl").distinct().rdd.map(x => x(0).toString).zipWithIndex.map(x => Row(x._1.toString, x._2.toString))
    val schema = StructType(Array(StructField("fullurl", StringType, true), StructField("id", StringType, true)))
    val fullurl_id_df = sqlContext.createDataFrame(fullurl_id, schema)
    WriteDB.writeData(fullurl_id_df, fullurlWithid)
    fullurl_id_df
  }
}
