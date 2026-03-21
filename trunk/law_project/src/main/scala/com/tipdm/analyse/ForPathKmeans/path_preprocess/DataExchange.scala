package com.tipdm.analyse.ForPathKmeans.path_preprocess

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

/**
  * suling
  * 数据变换
  * （1）	将同一个用户的所有轨迹点串成一条轨迹
  * （2）	以记录中出现的网页个数创建数组，将用户对轨迹点的兴趣度写入轨迹位置对应的数组位置，数组位置没有对应轨迹的记为0
  * 用户对某网页的兴趣度计算公式：intest(l.url)=m-l+1,其中m为网页个数，l为该网页在该用户轨迹中的位置，思想是用户越先访问的网页，兴趣度越高
  * 由于网页数太多，所以计算时使用的是用户访问的最多次数做为网页个数
  *
  */
object DataExchange {
  var sqlContext: HiveContext = null

  def getArray(arr: Seq[String], m: Int): Seq[Double] = {
    // 不定长轨迹转换为定长轨迹
    var intestArray = Array.fill(m)(-1.0)
    for (i <- 0 until arr.length; if i < m) {
      intestArray(i) = arr(i).toDouble
    }
    return intestArray.toSeq
  }

  def exchange(data: DataFrame, m: Int): DataFrame = {
    // 将所有的轨迹分割，计算兴趣度，并转换为定长事务
    // 注册自定义函数
    import org.apache.spark.mllib.linalg.Vectors
    sqlContext.udf.register("udf_getArray", (arr: Seq[String], m: Int) => Vectors.dense(getArray(arr, m).toArray)) // 数组长度
    sqlContext.udf.register("udf_getArray_String", (arr: Seq[String], m: Int) => getArray(arr, m).toArray.mkString(","))
    data.withColumn("furls_arr", split(col("fullurls"), ","))
      .selectExpr(userid, " fullurls", "udf_getArray(furls_arr," + m + ") as " + features, "udf_getArray_String(furls_arr," + m + ") as features_string")
  }

  def dataExchange(data: DataFrame, m: Int) = {
    val fullurl_id_df = URLMapID.MapURLToID(data, sqlContext)
    val data_concat = ConnectURL.concat_url(data.join(fullurl_id_df, fullurl), "id", "fullurls", ",")
    val data_exchange = exchange(data_concat, m)
    data_exchange
  }

  def setSQLContext(context: HiveContext) = {
    sqlContext = context
  }

  def main(args: Array[String]): Unit = {
    val context = getSparkSession("data exchange")
    setSQLContext(context)
    val data = ReadDB.getData(sqlContext)
    val data_clean = DataClean.clean(data)
    val data_specification = AttributeSpecification.specificate(data_clean, 5, context)
    val data_exchange = dataExchange(data_specification, 5)
    data_exchange.show(10, false)
  }
}
