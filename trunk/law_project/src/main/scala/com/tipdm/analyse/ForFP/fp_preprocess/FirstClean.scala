package com.tipdm.analyse.ForFP.fp_preprocess

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

object FirstClean {
  /**
    * 读取全量数据，并过滤访问次数低于2的用户记录
    *
    * @param data  全量数据lawtime_all
    * @param table 用于存放用户点击次数大于2的用户记录law_init1.lawtime_gt_one
    * @return
    */
  def cleanOnlyOne(data: DataFrame, table: String, sqlContext: HiveContext) = {
    //  访问次数大于1的数据
    val gt_one_data_userid = data.groupBy(userid).agg(count(userid) as "num").filter("num>1").select(userid)
    //  所有数据和访问次数大于1的记录做内连接，并注册临时表
    data.join(gt_one_data_userid, userid).select("realip", "realareacode", "useragent", "useros", "userid", "clientid", "timestamps", "timestamp_format", "pagepath", "ymd", "fullurl", "fullurlid", "hostname", "pagetitle", "pagetitlecategoryid", "pagetitlecategoryname", "pagetitlekw", "fullreferrrer", "fullreferrerurl", "organickeyword", "source").registerTempTable("fansy_a")
    //  从临时表建立正式表；
    sqlContext.sql("drop table IF EXISTS " + table)
    sqlContext.sql("create table " + table + " as select * from fansy_a")
  }

  /**
    * 用户记录去重并过滤点击次数在1以上的数据
    *
    * @param input  点击次数在1以上的用户记录
    * @param output 去重后数据存放的hive表名
    * @return
    */
  def cleanDuplicate(input: String, output: String, sqlContext: HiveContext): DataFrame = {
    // 1.去重数据join 访问次数大于1的记录
    val distinct_data_userid = sqlContext.sql("select realip,realareacode,useragent,useros,userid,clientid,timestamp_format,pagepath,ymd,fullurl,fullurlid,hostname,pagetitle,pagetitlecategoryid,pagetitlecategoryname,pagetitlekw,fullreferrrer,fullreferrerurl,organickeyword,source from " + input).distinct
    // 2. 去重后，访问次数大于1的记录
    val gt_one = distinct_data_userid.groupBy(userid).agg(count("userid") as "u_num").filter(("u_num >1")).select(col(userid) as "userid1")
    // 3. join两个数据
    distinct_data_userid.join(gt_one, distinct_data_userid(userid) === gt_one("userid1"), "inner").select("realip", "realareacode", "useragent", "useros", "userid", "clientid", "timestamp_format", "pagepath", "ymd", "fullurl", "fullurlid", "hostname", "pagetitle", "pagetitlecategoryid", "pagetitlecategoryname", "pagetitlekw", "fullreferrrer", "fullreferrerurl", "organickeyword", "source").registerTempTable("fansy_b")
    // 4. 生成新表
    sqlContext.sql("drop table IF EXISTS " + output)
    sqlContext.sql("create table " + output + " as select * from fansy_b")
  }

  def main(args: Array[String]): Unit = {
    val sqlContext = getSparkSession("firstClean")
    val data = sqlContext.sql("select * from " + args(0))
    cleanOnlyOne(data, args(1), sqlContext)
    cleanDuplicate(args(1), args(2), sqlContext)
  }
}
