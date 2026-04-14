package com.tipdm.analyse.ForPathKmeans.path_model

import com.tipdm.analyse.ForPathKmeans.path_preprocess.{AttributeSpecification, DataClean, DataExchange, ReadDB}
import com.tipdm.util.CommonUtil._
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * suling
  * 轨迹聚类：KMeans模型
  * 聚类后的聚类中心主要是用户兴趣度相似的集合，需要经过进一步处理才能使用，处理如下
  * 1.先找出同一类用户的访问轨迹
  * 2.对应位置的网页分别统计出现的次数，次数最大的作为这一类轨迹的第一个轨迹点，依次得到第二个、第三个、第n个轨迹点，将这些轨迹点串起来作为这一类用户的轨迹
  */
object KMeansModelTest {
  def create_model(data: DataFrame, K: Int) = {
    //建模
    val kmeans = new KMeans().setFeaturesCol(features).setK(K).setSeed(100L).setPredictionCol(predict_col)
    val model = kmeans.fit(data)
    model
  }

  def tmp1(clusterid: Double, arr: Array[String]): List[((Double, Int), String)] = {
    var list: List[((Double, Int), String)] = List()
    for (i <- 0 until arr.length) {
      list = list :+ ((clusterid, i), arr(i))
    }
    return list
  }

  def getMax(a: (String, Int), b: (String, Int)): (String, Int) = {
    if (a._2 > b._2)
      return a
    else
      return b
  }

  def analyse_center(model: KMeansModel, data: DataFrame) = {
    //聚类中心计算
    val predictions = model.transform(data)
    val pre_data = predictions.withColumn("furl_arr", split(col("fullurls"), ","))
    val tmp = pre_data.rdd.map(x => (x(4).toString.toDouble, x(1).toString.split(","))).flatMap { x => tmp1(x._1, x._2) }.map(x => ((x._1._1, x._1._2, x._2), 1))
    //结果：（（cid，index），furl）=>（(cid，index，furl),1）
    val result = tmp.reduceByKey((a, b) => a + b).map(x => ((x._1._1, x._1._2), (x._1._3, x._2))).reduceByKey((a, b) => getMax(a, b)).sortByKey().map(x => (x._1._1, x._2._1)).reduceByKey((a, b) => a + "," + b)
    result
  }

  def main(args: Array[String]): Unit = {
    val sqlContext = getSparkSession("kmeans model")
    val data = ReadDB.getData(sqlContext)
    val data_clean = DataClean.clean(data)
    val data_specification = AttributeSpecification.specificate(data_clean, 28, sqlContext)
    DataExchange.setSQLContext(sqlContext)
    val data_exchange = DataExchange.dataExchange(data_specification, 28)
    val model = create_model(data_exchange, 10)
    model.write.overwrite().save(modelPath)
    val centers = analyse_center(model, data_exchange)
    centers.saveAsTextFile(centerPath)

  }
}
