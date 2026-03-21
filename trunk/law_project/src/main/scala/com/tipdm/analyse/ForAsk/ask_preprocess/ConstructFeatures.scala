package com.tipdm.analyse.ForAsk.ask_preprocess

import java.text.SimpleDateFormat

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

/**
  * //@Author: fansy 
  * //@Time: 2018/9/20 13:46
  * //@Email: fansy1990@foxmail.com
  *
  * 构造特征列：
  * 1. 用户成功发布前的访问次数；
  * 2. 用户成功发布前的访问时间； 考虑是否可以做些时间间隔等来构造多几个特征列
  * 3. 用户成功发布前的相关网页访问次数，如：http://www.lawtime.cn/ask/ask.php
  *
  * 目标列：
  * 4. 如果有发布，则为1，否则为0；
  *
  */
object ConstructFeatures {

  val timestamp_format = "timestamp_format"
  val fullurl = "fullurl"
  val askSuccess_keywork = "askSuccess"
  val relevant_url = "http://www.lawtime.cn/ask/ask.php"

  /**
    *
    * @param data
    * +--------------------+-------------------+--------------------+---------+--------------------+-------------------+---------------------+-----------+
    * |              userid|   timestamp_format|             fullurl|fullurlid|           pagetitle|pagetitlecategoryid|pagetitlecategoryname|pagetitlekw|
    * +--------------------+-------------------+--------------------+---------+--------------------+-------------------+---------------------+-----------+
    * |109534721.1409175973|2014-08-28 05:57:15|http://www.lawtim...|  1999001|瓦房店律师-法律快车专业瓦房店律师...|                 21|                 医患纠纷|       法律咨询|
    * |109534721.1409175973|2014-08-28 05:57:47|http://www.lawtim...|   102002|瓦房店劳动工伤律师-法律快车瓦房店...|                 68|               工伤赔偿纠纷|       法律咨询|
    * +--------------------+-------------------+--------------------+---------+--------------------+-------------------+---------------------+-----------+
    * @return
    *
    */
  def getConstructFeatures(data: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions._
    val combine_udf = udf { (timestamp_format: String, fullurl: String, pagetitle: String) => { // 1: askSuccess_keywork; 2: relevant ; 0 : none
      if (pagetitle != null && pagetitle.contains("咨询发布成功")) {
        timestamp_format + "_1"
      } else if (fullurl != null && fullurl.contains(askSuccess_keywork)) {
        timestamp_format + "_1"
      } else if (fullurl != null && fullurl.contains(relevant_url)) {
        timestamp_format + "_2"
      } else {
        timestamp_format + "_0"
      }
    }
    }
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val cal_udf = udf { (data: mutable.WrappedArray[String]) => {
      val hasAskSuccess = (data.filter(x => x.contains("_1")).length > 0)
      val splitted_data = data.map { x => val t = x.split("_"); (t(0), t(1).toInt) }
      if (hasAskSuccess) {
        val sorted_data = splitted_data.sortBy(x => x._1)
        val sub_data = sorted_data.takeWhile(x => x._2 != 1) // askSuccess 前的数据
        if (sub_data.length < 1) {
          Array(0.0, 0, 0, -1)
        } else {
          Array((simpleDateFormat.parse(sub_data.last._1).getTime -
            simpleDateFormat.parse(sub_data.head._1).getTime) / 60 / 1000.0,
            sub_data.length, sub_data.filter(x => x._2 == 2).length, 1)
        }
      } else {
        val max_time_string = splitted_data.map(x => x._1).max
        val min_time_string = splitted_data.map(x => x._1).min
        val count = data.length
        val relevant_count = splitted_data.filter(x => x._2 == 2).length
        Array((simpleDateFormat.parse(max_time_string).getTime -
          simpleDateFormat.parse(min_time_string).getTime) / 60 / 1000.0, count, relevant_count, 0
        )
      }
    }
    }
    val constructed_data = data.select(col(userid), combine_udf(col(timestamp_format), col(fullurl), col("pagetitle")) as timestamp_format).groupBy(userid).agg(collect_list(col(timestamp_format)) as timestamp_format).select(col(userid), cal_udf(col(timestamp_format)) as features)
    constructed_data
  }


  def main(args: Array[String]): Unit = {
    val data = ReadDB.getData()
    val features_data = getConstructFeatures(data)
    features_data.show(false)
  }
}
