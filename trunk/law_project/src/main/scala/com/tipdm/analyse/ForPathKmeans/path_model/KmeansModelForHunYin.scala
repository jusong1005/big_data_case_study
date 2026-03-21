package com.tipdm.analyse.ForPathKmeans.path_model

import com.tipdm.analyse.ForPathKmeans.path_preprocess._
import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{collect_set, concat_ws}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object KmeansModelForHunYin {
  def centerExchange(centerPath: String, fullurl_id: String, sqlContext: HiveContext) = {
    val center = sqlContext.sparkContext.textFile(centerPath).map(x => x.slice(1, x.length - 2)).map { x => val line = x.split(","); (line(0), line.slice(1, line.length - 1)) }
      .flatMap(f => for (i <- 0 until f._2.length) yield (f._1, f._2(i))).map(x => Row(x._1, x._2))
    val schema = StructType(Array(StructField("clusterID", StringType, true), StructField("fullurlID", StringType, true)))
    val centerDF = sqlContext.createDataFrame(center, schema)
    centerDF.show(false)
    val furl_id = ReadDB.getData(fullurl_id, sqlContext)
    furl_id.show(false)
    val center_url = centerDF.join(furl_id, centerDF("fullurlID") === furl_id("id"), "inner")
    center_url.groupBy("clusterID").agg(concat_ws("|||", collect_set(fullurl)) as "center_url").select("clusterID", "center_url").show(false)
  }

  def main(args: Array[String]): Unit = {
    val sqlContext = getSparkSession("kmeans model")
    centerExchange(centerPath, fullurlWithid, sqlContext)
  }

}
