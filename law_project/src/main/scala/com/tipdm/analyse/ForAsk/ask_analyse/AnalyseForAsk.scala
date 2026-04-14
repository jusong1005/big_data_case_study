package com.tipdm.analyse.ForAsk.ask_analyse

import com.tipdm.analyse.AnalyseForBasis.handle_args
import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
/**
  * 用户发布咨询成功预测的相关探索
  * //@Author: suling
  */
object AnalyseForAsk {

  /**
    * 网页类别规则探索，根据人工分析，每种网页类型对应的网页都具有一定的规则，现在探索网页类型与网页规则与人工发现是否有出入
    * @param data law_init1.lawtime_gt_one_distinct
    */
  def analyseUrlRule(data:DataFrame)= {
    val rule = data.selectExpr(fullurlid,"case when(fullurlid=101001 and (fullurl like '%http://www.lawtime.cn/ask/%')) then '101001' " +
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
      "when (fullurlid=301001 and (fullurl like '%http://law.lawtime.cn/%')) then '301001' " +
      "when (fullurlid=1999001 and (fullurl like '%http://www.lawtime.cn/ask/index.php?m=tips&a=askSuccess%')) then '1999001-success' " +
      "when (fullurlid=1999001) then '1999001' " +
      "else fullurl end as url").distinct().orderBy(fullurlid)
    rule.show(200, false)
  }

  /**
    *探索咨询发布成功页面的特点
    * @param table law_init1.lawtime_gt_one_distinct
    * @return
    */
  def analyseAskSuccess(table:String,sqlContext:HiveContext) = {
    // 查看网址含有askSuccess的页面标题有哪些
    val askSuccess = sqlContext.sql("select  pagetitle, count(1) from "+table+" where fullurl like '%askSuccess%' group by pagetitle")
    askSuccess.show(20, false)
    // 查看标题含有咨询发布成功，网址包含askSuccess的网址有哪些
    val page = sqlContext.sql("select  distinct(fullurl), count(1) from " + table + " where pagetitle like '咨询发布成功'  and fullurl not like  '%askSuccess%' group by fullurl")
    page.show(10, false)
  }

  /**
    * 统计发布成功用户访问页面的类型分布
    * @param data law_init1.lawtime_gt_one_distinct
    */
  def analyseAskSucessUser(data:DataFrame)= {
    val users = data.filter("fullurl like '%askSuccess%'").select(userid).distinct()
    val data2 = data.join(users, userid)
    val data3 = data2.groupBy(fullurlid).agg(count(fullurlid) as "count").orderBy(desc("count"))
    data3.show(50, false)
  }

  /**
    * 统计发布咨询用户访问律师界面的概率
    * @param data law_init1.lawtime_gt_one_distinct
    */
  def analyseVisitLaw(data:DataFrame) = {
    val users = data.filter("fullurl like '%askSuccess%'").select(userid).distinct()
    val data2 = data.join(users,userid)
    val user_law = data2.filter("fullurl like '%lawyer%' or (fullurl like '%lawfirm%') or (fullurl like '%interview%') or (fullurl like '%mylawyer%')")
      .select(userid).distinct()
    println("访问概率：" + user_law.count()*100.0/users.count())
  }
  /**
    * 统计咨询发布成功页面的数据量以及发布咨询成功的用户数
    * @param data law_init1.lawtime_gt_one_distinct
    */
  def analyseAskNum(data:DataFrame) = {
    val dataNum = data.filter("fullurlid='1999001' and fullurl like '%http://www.lawtime.cn/ask/index.php?m=tips&a=askSuccess%'").count * 100.0 / data.filter("fullurlid = 1999001").count
    val userNum = data.filter(fullurlid+"=1999001 and (fullurl like '%http://www.lawtime.cn/ask/index.php?m=tips&a=askSuccess%')").select("userid").distinct.count * 100.0 / data.select("userid").distinct.count
    println("咨询发布成功的用户记录数：" + dataNum)
    println("成功发布咨询的用户数：" + userNum)
  }

  def main(args: Array[String]): Unit = {
    //初始化
    val (appName,inputTable) = handle_args(args)
    val sc = new SparkContext(new SparkConf().setAppName(appName))
    val sqlContext = new HiveContext(sc)
    val data = sqlContext.sql(s"select * from $inputTable").cache()
    analyseUrlRule(data)
    analyseAskSucessUser(data)
    analyseAskNum(data)
    analyseVisitLaw(data)

  }
}
