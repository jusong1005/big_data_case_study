package com.tipdm.analyse.ForFP.fp_analyse

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.functions._

/** suling
  * 对网址类型的探索分析
  */
object AnalyseFullUrlID {
  val sqlContext = getSparkSession("analyse fullurlid")

  /**
    * 网页类别规则探索，根据人工分析，每种网页类型对应的网页都具有一定的规则，现在探索网页类型与网页规则与人工发现是否有出入
    *
    * @param table law_init1.lawtime_gt_one_distinct
    */
  def analyseUrlRule(table: String) = {
    val data = sqlContext.sql("select * from " + table)
    val rule = data.selectExpr(fullurlid, "case when(fullurlid=101001 and (fullurl like '%http://www.lawtime.cn/ask/%')) then '101001' " +
      "when (fullurlid=101002 and (fullurl like '%http://www.lawtime.cn/ask/browse%')) then '101002' " +
      "when (fullurlid=101003 and (fullurl like '%http://www.lawtime.cn/ask/question%')) then '101003' " +
      "when (fullurlid=101004 and (fullurl like '%http://www.lawtime.cn/ask/expert/%')) then '101004' " +
      "when (fullurlid=101005 and (fullurl like '%http://www.lawtime.cn/ask/moderator%')) then '101005' " +
      "when (fullurlid=101006 and (fullurl like '%http://www.lawtime.cn/ask/anstar%')) then '101006' " +
      "when (fullurlid=101007 and (fullurl like '%http://www.lawtime.cn/ask/ranking.html%')) then '101007' " +
      "when (fullurlid=101008 and (fullurl like '%http://www.lawtime.cn/ask/online/%')) then '101008' " +
      "when (fullurlid=101009 and (fullurl like '%http://www.lawtime.cn/ask/exp/%')) then '101009' " +
      "when (fullurlid=102001 and (fullurl like '%http://www.lawtime.cn/%/lawyer%')) then '102001' " +
      "when (fullurlid=102002 and (fullurl like 'http://www.lawtime.cn/%/lawyer/p%ll%')) then '102002' " +
      "when (fullurlid=102003 and (fullurl like 'http://www.lawtime.cn/%/online')) then '102003' " +
      "when (fullurlid=102004 and (fullurl like 'http://www.lawtime.cn/%/lawyer/area')) then '102004' " +
      "when (fullurlid=102005 and (fullurl like 'http://www.lawtime.cn/%/lawyer/pro')) then '102005' " +
      "when (fullurlid=102006 and (fullurl like 'http://www.lawtime.cn/%/lawyer/love')) then '102006' " +
      "when (fullurlid=102007 and (fullurl like 'http://www.lawtime.cn/%/lawyer/article')) then '102007' " +
      "when (fullurlid=102008 and (fullurl like 'http://www.lawtime.cn/%/lawyer/case')) then '102008' " +
      "when (fullurlid=102009 and (fullurl like 'http://www.lawtime.cn/%/lawyer/team')) then '102009' " +
      "when (fullurlid=103002 and (fullurl like 'http://www.lawtime.cn/interview/list.html')) then '103002' " +
      "when (fullurlid=103003 and (fullurl like 'http://www.lawtime.cn/interview/article/%.html')) then '103003' " +
      "when (fullurlid=106001 and (fullurl like 'http://www.lawtime.cn/%/lawfirm')) then '106001' " +
      "when (fullurlid=107001 and (fullurl like '%http://www.lawtime.cn/info/%')) then '107001' " +
      // "when (fullurlid=201001 then fullurl " +
      "when (fullurlid=301001 and (fullurl like '%http://law.lawtime.cn/%')) then '301001' " +
      "when (fullurlid=1999001 and (fullurl like '%http://www.lawtime.cn/ask/index.php?m=tips&a=askSuccess%')) then '1999001-success' " +
      "when (fullurlid=1999001) then '1999001' " +
      "else fullurl end as url").distinct().orderBy(fullurlid)
    rule.show(200, false)
  }

  /**
    * 探索各类型网页的分布情况
    *
    * @param table law_init1.lawtime_gt_one_distinct
    */
  def analyseUrlIdDistribute(table: String) = {
    val data = sqlContext.sql("select * from " + table)
    val data_count = data.count
    val distribute = data.select(fullurlid).groupBy(fullurlid).agg(count(fullurlid) as "url_count", count(fullurlid) * 100.0 / data_count as "url_count_percent").orderBy(desc("url_count"))
    distribute.show(30, false)
  }


  /**
    * 探索网址标题类型及类型ID
    *
    * @param table law_init1.lawtime_gt_one_distinct
    */
  def analysePagetitle(table: String) = {
    val data = sqlContext.sql("select *from " + table).select("userid", "timestamp_format", "fullurl", "fullurlid", "pagetitle", "pagetitlecategoryid", "pagetitlecategoryname", "pagetitlekw")
    val data1 = data.selectExpr("pagetitlecategoryid  as ptc", "pagetitlecategoryname as ptcn").distinct.filter("ptcn != '0'")
    data1.show(80, false)
  }

  /**
    * 探索网址类型与网页类型的关系
    */
  def analyseFIDAndPID(table: String) = {
    val data = sqlContext.sql("select * from " + table)
    val fid_pid = data.select(fullurlid, "pagetitlecategoryid").distinct().orderBy(fullurlid)
    fid_pid.show(100, false)
    val fiscount = fid_pid.groupBy(fullurlid).agg(count(fullurlid) as "pcount").orderBy(fullurlid)
    fiscount.show(100, false)
  }

  /**
    * 统计网页类型的前3个值对应的大类型中包含的记录数
    *
    * @param table law_init1.lawtime_gt_one_distinct
    * @return
    */
  def analyseFirstThreeUrlId(table: String) = {
    val data = sqlContext.sql("select * from " + table)
    val num = data.count()
    val urlCount = data.groupBy(substring(col(fullurlid), 0, 3) as "suburlid").agg(count(fullurl) as "dcount", count(fullurl) * 100.0 / num as "dcount_precent").orderBy(desc("dcount"))
    urlCount
  }

  /**
    * 统计每个大类里面每个小类包含的记录数
    *
    * @param table law_init1.lawtime_gt_one_distinct
    * @return
    */
  def analyseUrlIdInner(table: String) = {
    val data = sqlContext.sql("select * from " + table)
    val num = data.count()
    val urlidCount = data.groupBy(substring(col(fullurlid), 0, 3) as "suburlid", col(fullurlid)).agg(count(fullurl) as "dcount", count(fullurl) * 100.0 / num as "dcount_precent").orderBy(col("suburlid"), desc("dcount"))
    urlidCount
  }

  def analyse107001(table: String) = {
    val data = sqlContext.sql("select * from " + table).filter(fullurlid + "=107001")
    val num = data.count()
    val knowledge = data.selectExpr(fullurl, "case when(fullurl like '%http://www.lawtime.cn/info/%.html') then '知识内容页' " +
      "when (fullurl like '%http://www.lawtime.cn/info/%/') then '知识首页' " +
      "else '知识列表页' end as type").groupBy("type").agg(count(fullurl) as "dcount", count(fullurl) * 100.0 / num as "dcount_percent").orderBy(desc("dcount"))
    knowledge.show(10, false)
  }

  def main(args: Array[String]): Unit = {
    val urlidThree = analyseFirstThreeUrlId(table_gt_oneclick_data_distinct)
    urlidThree.show(20, false)
    val urlCount = analyseUrlIdInner(table_gt_oneclick_data_distinct)
    urlCount.show(50, false)
    analyse107001(table_gt_oneclick_data_distinct)
  }
}
