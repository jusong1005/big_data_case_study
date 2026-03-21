package com.tipdm.analyse.ForPathKmeans.path_preprocess

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * suling
  * 过滤访问次数仅一次的用户记录
  */
object CleanOnlyOneClick {

  def cleanOnlyOne(data: DataFrame, num: Int) = {
    val userOnlyOne = data.groupBy(userid).agg(count(fullurl) as "fcount").filter("fcount>" + num).select(userid).distinct()
    data.join(userOnlyOne, userid)
  }

}
