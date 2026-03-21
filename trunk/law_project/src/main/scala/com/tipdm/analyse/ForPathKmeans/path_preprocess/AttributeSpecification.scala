package com.tipdm.analyse.ForPathKmeans.path_preprocess

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

/**
  * suling
  * 属性规约
  * 1.	选择用户ID、网址、时间三个字段的数据
  *2.	按照时间对每个用户的数据进行排序
  *3.	用户记录超过两百次的删除
  *
  */
object AttributeSpecification {
  def specificate(data: DataFrame, num: Int, sqlContext: HiveContext): DataFrame = {
    val data_select = data.select(userid, fullurl, timestamp_format).orderBy(timestamp_format)
    data_select.registerTempTable("tmp")
    sqlContext.sql("select temp.userid,temp.fullurl,temp.timestamp_format from (select userid,fullurl,timestamp_format,row_number() over(partition by userid order by timestamp_format) users from tmp) temp where temp.users<=" + num)

  }

  def main(args: Array[String]): Unit = {
    val sqlContext = getSparkSession("Attribute Specification")
    val data = ReadDB.getData(sqlContext)
    val cleanData = DataClean.clean(data)
    val specificationData = specificate(cleanData, 200, sqlContext)
    specificationData.show(10, false)
  }
}
