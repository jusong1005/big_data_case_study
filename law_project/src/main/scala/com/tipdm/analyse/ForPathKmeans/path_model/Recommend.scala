package com.tipdm.analyse.ForPathKmeans.path_model

import com.tipdm.analyse.ForPathKmeans.path_preprocess.{DataClean, DataExchange, ReadDB}
import com.tipdm.util.CommonUtil._
import org.apache.spark._
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.functions.{count, _}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * suling
  * 个性化推荐
  * （1）	当一个用户进行访问时，根据获取的用户路径，将其转换为兴趣度，再利用聚类模型进行预测。
  * （2）	将该路径所属分类的所有路径取出，剔除用户访问过的路径，剩余的路径形成推荐列表推荐给用户。
  *
  */
object Recommend {
  //准备test，从原先清理好的数据中随意选出访问次数大于10的5%的用户数据，用户记录进行清理，每个用户选择前10个访问路劲作为测试轨迹，剩余部分用于结果的评价。
  def getTest(data: DataFrame, num: Int) = {
    val fullurl_gt_num = data.groupBy(userid).agg(count(fullurl) as "fcount").filter("fcount>" + num).select(userid).distinct()
    val Array(testuser, valibuser) = fullurl_gt_num.randomSplit(Array(0.05, 0.1))
    val fullurl_number = getSparkSession().read.format("jdbc").options(Map("url" -> "jdbc:mysql://192.168.0.109:3306/law_init", "driver" -> "com.mysql.jdbc.Driver", "dbtable" -> fullurlWithid, "user" -> "root", "password" -> "root")).load()
    val data_num_gt_ten = data.join(testuser, userid).join(fullurl_number, fullurl)
    val test = data_num_gt_ten.groupBy(userid).agg(concat_ws(",", collect_set("id")) as "fullurls").select(userid, "fullurls").rdd.map { x => val furl = x(1).toString.split(","); Row(x(0).toString, furl.slice(0, 10), furl.slice(10, furl.length)) }
    val schema = StructType(Array(StructField("userid", StringType, true), StructField("furls_arr", ArrayType(StringType, true), true), StructField("valib_arr", ArrayType(StringType, true), true)))
    val testData = getSparkSession().createDataFrame(test, schema)
    getSparkSession().udf.register("udf_getArray", (arr: Seq[String], m: Int) => Vectors.dense(DataExchange.getArray(arr, m).toArray))
    //数组长度
    val test_id = testData.selectExpr("userid", "furls_arr", "valib_arr", "udf_getArray(furls_arr,28) as features")
    test_id
  }

  // 评估
  // 定义一个求交集的udf
  def getIntersect(a: Seq[String], b: Seq[String]): Seq[String] = {
    val in = a intersect b
    return in.toSeq
  }

  // 定义一个求差集的udf
  def getDiff(a: Seq[String], b: Seq[String]): Seq[String] = {
    val di = a diff b
    return di.toSeq
  }

  def getLen(a: Seq[String]): Int = {
    return a.length
  }

  def registerFun() = {
    // 注册函数
    getSparkSession().udf.register("getIntersect", (a: Seq[String], b: Seq[String]) => getIntersect(a, b))
    getSparkSession().udf.register("getDiff", (a: Seq[String], b: Seq[String]) => getDiff(a, b))
    getSparkSession().udf.register("getLen", (a: Seq[String]) => getLen(a))
  }

  def predictModel(data: DataFrame) = {
    val model = KMeansModel.load(modelPath)
    val predict = model.transform(data)
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("ttt"))
    val kmeans_center = sc.textFile(centerPath).map { x => val line = x.slice(1, x.length - 1).split(","); Row(line(0), line.slice(1, line.length)) }
    val schema2 = StructType(Array(StructField("prediction", StringType, true), StructField("cluster_url", StringType, true)))
    val kmeans_centers = getSparkSession().createDataFrame(kmeans_center, schema2)
    val pre_center = predict.join(kmeans_centers, "prediction")
    pre_center
  }

  def executor(pre_center: DataFrame) = {
    registerFun()
    val recommend = pre_center.selectExpr("userid", "getIntersect(valib_arr, cluster_url) as zhengquetuijian", "getDiff(cluster_url,furls_arr) as ketuijian")
    val accuary = recommend.selectExpr("sum(getLen(zhengquetuijian)) as good", " sum(getLen(ketuijian)) as allurl").rdd.map(x => x(0).toString.toDouble / x(1).toString.toDouble * 100)
    (recommend, accuary)
  }

  def main(args: Array[String]): Unit = {
    val data = ReadDB.getData()
    val data_clean = DataClean.clean(data)
    val testData = getTest(data_clean, 10)
    testData.show(false)
  }
}
