package com.tipdm.model

import com.tipdm.util.CommonUtil._
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object ALSModelRecommend {
  /*
  过滤N个推荐的结果
   */
  def recNumSelect(x: List[(Int, Double)], recNum: Int): List[(Int, Double)] = {
    if (x.length <= 0) null
    else if (x.length > 0 && x.length < recNum) return x
    else return x.slice(0, recNum)
  }

  // 推荐
  def recommend(model: MatrixFactorizationModel, recNum: Int, data: RDD[(Int, Int)]): RDD[(Int, Int)] = {
    val result = model.predict(data).map(x => (x.user, (x.product, x.rating))).combineByKey(
      a => List(a),
      (b: List[(Int, Double)], a) => b.::(a),
      (a: List[(Int, Double)], b: List[(Int, Double)]) => a ::: b
    ).map(x => (x._1, x._2.sortWith((y1, y2) => y1._2 > y2._2))).mapValues(x => recNumSelect(x, recNum)).filter(x => x._2 != null)
      .flatMap { x =>
        for (rec <- x._2)
          yield (x._1, rec._1)
      }
    result
  }

  // 反编码
  def encodeToFullUrl(data: DataFrame, usermeta: DataFrame, itemmeta: DataFrame, datauid: String,
                      datapid: String, metauid: String, metapid: String, metauser: String, metaitem: String) = {
    val data1 = data.join(usermeta, data(datauid) === usermeta(metauid), "inner")
    val data2 = data1.join(itemmeta, data1(datapid) === itemmeta(metapid), "inner")
    data2.select(metauser, metaitem)
  }

  def parseArgs(args: Array[String]) = {
    (args(0), args(1).toInt, args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11))
  }

  def main(args: Array[String]): Unit = {
    val (modelPath, recNum, usermetaTable, itemmetaTable, datauid,
    datapid, metauid, metapid, metauser, metaitem, outPath, trainTable) = parseArgs(args)
    val sc = new SparkContext(new SparkConf().setAppName("ALS Recommend"))
    val sqlContext = new HiveContext(sc)
    import sqlContext.implicits._
    val model = MatrixFactorizationModel.load(sc, modelPath)
    val data = sqlContext.sql(s"select $datauid,$datapid from $trainTable").rdd.map(x => (x.getLong(0).toInt, x.getLong(1).toInt))
    val result = recommend(model, recNum, data).toDF(datauid, datapid)
    val usermeta = sqlContext.sql(s"select * from $usermetaTable")
    val itemmeta = sqlContext.sql(s"select * from $itemmetaTable")
    val data_recommend = encodeToFullUrl(result, usermeta, itemmeta, datauid, datapid, metauid, metapid, metauser, metaitem)
    val result2 = data_recommend.rdd.map(x => (x.getString(0), x.getString(1))).reduceByKey((x, y) => x + "|||" + y).map(x => x._1 + "|||" + x._2)
    saveAsHDFSFile(result2, outPath, true, sqlContext)
  }
}
