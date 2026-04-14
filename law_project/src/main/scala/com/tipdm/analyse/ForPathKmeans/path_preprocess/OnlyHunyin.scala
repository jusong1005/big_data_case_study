package com.tipdm.analyse.ForPathKmeans.path_preprocess

import com.tipdm.analyse.ForPathKmeans.readOrwriteData.ReadHiveData
import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame

/**
  * suling
  * 不同地区的用户浏览习惯可能不一样，关注不同内容的用户浏览习惯也不一样
  * 之前采取全量数据计算的效果比较差，现在仅选择广州的用户浏览数据进行分析
  * 在所有的浏览中，咨询跟知识类别的浏览是比较多的，其余内容相对较少，选择知识中的婚姻内容浏览记录
  */
object OnlyHunyin {
  def getData(data: DataFrame, keyword: String) = {
    val data_hunyin = data.filter("realareacode like '140100'").filter(fullurl + " like '%" + keyword + "%'")
    data_hunyin
  }

  def analyse(data: DataFrame) = {
    println("数据量：" + data.count())
    println("网页数：" + data.select(fullurl).distinct().count())
  }

  def main(args: Array[String]): Unit = {
    val data = ReadHiveData.readData(table_gt_oneclick_data_distinct)
    val data_hunyin = getData(data, "info/hunyin")
    analyse(data_hunyin)
    val data_clean = DataClean.clean(data_hunyin)
    analyse(data_clean)
  }
}
