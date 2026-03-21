package com.tipdm.dataclean

import com.tipdm.util.CommonUtil._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 模型构建需要的数据处理公共处理部分，包括的内容：
  * （1）第一步是进行重复数据跟访问一次的用户记录处理，这部分的操作在数据探索的过程中已经先行处理过了，处理后的数据在保存在Hive表law_init1.lawtime_gt_one及law_init1.lawtime_gt_one_distinct中。在此基础上进行了数据的探索分析。
  * （2）网页分类比较多，每一类的数据访问都存在不同的特点，因此选择数据中占比很高的咨询内容的数据进行模型分析，即类型ID为101003的记录。
  * （3）过滤标题含有“咨询发布成功”字样的网页，或是网址中带有“askSuccess”字样的网页记录，因为这部分记录是用户发布咨询后跳出的网页，并非用户实际进行访问的网页。
  * （4）对咨询相关的数据中，网址存在的带“？”的网页网址进行处理，具体处理规则是以“？”为分割点，截取“？”前面的网址，数据存储在Hive表law_init1.data_101003_processed。
  * （5）之前的重复数据处理主要是基于整行记录，现基于用户ID、网址、时间戳三个字段进行去重处理。
  * （6）去重后的数据可能存在了新的一次访问记录，需要在进行一次新的一次访问记录过滤操作，数据存储在law_init1.data_101003_url_gt_one。
  * （7）对数据按照用户ID、网址、时间戳三个字段进行去重处理，过滤出访问次数大于等于2的用户记录，数据存储在law_init1.data_101003_real_url_gt_one。
  * （8）将用户ID、网页网址取出，各自进行编码，编码完成后使用用户ID编码以及网址编码替换原先的用户ID和网页网址，数据存储在law_init1.data_101003_encoded。
  * //（9）数据切割，按照8:1:1的比例将数据切分成训练集、验证集、测试集，分别存储在Hive表law_init1.data_101003_encoded_train，law_init1.data_101003_encoded_validate，law_init1.data_101003_encoded_test。
  *
  */
object DataPreprocess01_Public {
  /**
    * 这个方法的作用是根据输入的数据以及字段名称去重
    *
    * @param data    输入的数据DataFrame
    * @param columns 进行记录间重复值匹配的字段
    * @return 返回的是去重后的DataFrame
    */
  def cleanDuplication(data: DataFrame, columns: Array[String]) = {
    if (columns != null && columns.length > 0) {
      data.dropDuplicates(columns)
    }
    else {
      data.dropDuplicates()
    }
  }

  /**
    * 这个方法的作用是过滤点击次数低于num的用户记录
    *
    * @param data
    * @param num
    */
  def cleanClickLowNum(data: DataFrame, num: Int) = {
    val gt_one_data_userid = data.groupBy(userid).agg(count(userid) as "num").filter("num>" + num).select(userid)
    // 所有数据和访问次数大于1的记录做内连接，并注册临时表
    data.join(gt_one_data_userid, userid)
  }

  /**
    * 这个方法的作用是选择从原始数据中抽取最后的num天作为初始数据
    *
    * @param data
    * @param num
    */
  def getData(data: DataFrame, num: Int) = {
    val ymdNum = data.select("ymd").distinct().orderBy(desc("ymd")).limit(num)
    data.join(ymdNum, "ymd")
  }

  /**
    * 选择某一类别的相关数据
    *
    * @param data      去重以及去除一次访问的数据DataFrame
    * @param fullurlID 网页类别id
    * @return 符合要求的数据
    */
  def getAskData(data: DataFrame, fullurlID: String) = {
    // 101003的所有数据
    val data_101003 = data.filter("fullurlid = " + fullurlID)
    data_101003
  }

  /**
    * 删除某个字段含有某关键字的记录
    *
    * @param data      输入数据
    * @param keyColumn 字段
    * @param keyWord   关键字
    * @return 满足条件DataFrame
    */
  def filterSpecialData(data: DataFrame, keyColumn: String, keyWord: String) = {
    data.filter(keyColumn + " not like '%" + keyWord + "%'")
  }

  /**
    * 处理网址中的？
    *
    * @param data 咨询类别数据data_101003
    * @return
    */
  def filterWenhao(data: DataFrame, sqlContext: HiveContext) = {
    sqlContext.udf.register("getRealUrl", (url: String) => if (url.contains("?")) url.substring(0, url.indexOf("?")) else url)
    data.selectExpr("getRealUrl(fullurl) as fullurl", "userid", "timestamp_format")
  }

  case class User_ID(user: String, id: Long)

  case class Item_ID(item: String, id: Long)

  /**
    * 数据编码
    *
    * @param data
    * @param userCol       userid
    * @param itemCol       fullurl
    * @param userMetaTable user元数据
    * @param itemMetaTable item元数据
    * @return
    */
  def encode_data(sqlContext: HiveContext,
                  data: DataFrame, userCol: String, itemCol: String,
                  newUserCol: String, newItemCol: String,
                  userMetaTable: String, itemMetaTable: String): DataFrame = {
    import sqlContext.implicits._
    val allUsers = data.select(userCol).distinct().orderBy(userCol).rdd.map(_.getString(0)).zipWithIndex().map(x => User_ID(x._1, x._2)).toDF
    val allItems = data.select(itemCol).distinct().orderBy(itemCol).rdd.map(_.getString(0)).zipWithIndex().map(x => Item_ID(x._1, x._2)).toDF
    // save
    allUsers.registerTempTable("userMetaTable_00")
    allItems.registerTempTable("itemMetaTable_00")
    sqlContext.sql("create table " + userMetaTable + " as select * from userMetaTable_00")
    sqlContext.sql("create table " + itemMetaTable + " as select * from itemMetaTable_00")
    // join
    data.join(allUsers, data(userCol) === allUsers("user"), "leftouter").drop(allUsers("user")).withColumnRenamed("id", newUserCol).join(allItems, data(itemCol) === allItems("item"), "leftouter").drop(allItems("item")).withColumnRenamed("id", newItemCol)
  }

  /**
    * 分割为训练、测试集
    *
    * @param data
    * @param trainPercent
    * @param testPercent
    * @param sortedColumn
    * @return
    */
  def split2(data: DataFrame, trainPercent: Double, testPercent: Double, sortedColumn: String): (DataFrame, DataFrame) = {
    val (d1, d2, d3) = split3(data, trainPercent, testPercent, 0.0, sortedColumn)
    (d1, d2)
  }

  /**
    * 分割训练、验证、测试集
    *
    * @param data
    * @param trainPercent
    * @param validatePercent
    * @param testPercent
    * @param sortedColumn
    * @return
    */
  def split3(data: DataFrame, trainPercent: Double, validatePercent: Double, testPercent: Double, sortedColumn: String): (DataFrame, DataFrame, DataFrame) = {
    val percents = Array(trainPercent, validatePercent, testPercent)
    val all_size = data.count()
    val real_precents = percents.map(x => (x / percents.sum * all_size).toLong)
    //
    val allSortedColumnData = data.select(sortedColumn).orderBy(sortedColumn).rdd.map(_.getString(0)).zipWithIndex().map(x => (x._2, x._1))
    val firstSplitPoint = allSortedColumnData.lookup(real_precents(0)).head
    if (testPercent >= 0.0) {
      val secondSplitPoint = allSortedColumnData.lookup(real_precents(0) + real_precents(1)).head
      (
        data.filter(sortedColumn + " < '" + firstSplitPoint + "'"),
        data.filter(sortedColumn + " >= '" + firstSplitPoint + "' and " + sortedColumn + " < '" + secondSplitPoint + "'"),
        data.filter(sortedColumn + " >= '" + secondSplitPoint + "'")
      )
    } else {
      (
        data.filter(sortedColumn + " < " + firstSplitPoint),
        data.filter(sortedColumn + " >= " + firstSplitPoint),
        null
      )
    }
  }

  def parseArgs(args: Array[String]) = {
    // 参数分别为原始数据表，选择分析的数据天数，数据去重跟一次访问存储表，咨询相关数据存储表,处理了含askSuccess网页记录及？记录的数据存储，二次去重跟去除一次访问的数据存储，
    // useid,fullurl,新的userID，新的fullURL，user原数据，fullURL原数据,编码数据存储,训练集、验证集、测试集
    (args(0), args(1), args(2), args(3), args(4), args(5), args(6), args(7), args(8), args(9), args(10), args(11), args(12), args(13))
  }

  def main(args: Array[String]): Unit = {
    val (inputTable, times, output01, output02, output03, output04, userCol, itemCol, newUserCol, newItemCol, userMetaTable, itemMetaTable, output05, fullurlid) = parseArgs(args)
    //初始化
    val sc = new SparkContext(new SparkConf().setAppName("datapreprocss public"))
    val sqlContext = new HiveContext(sc)
    //读取原始数据
    val dataSource = sqlContext.sql("select * from " + inputTable) //保存增量数据的Hive表
    val data = getData(dataSource, times.toInt)
    //（1）进行重复数据跟访问一次的用户记录处理，处理后的数据在保存在Hive表law_init1.lawtime_gt_one_distinct中
    val data_to_duplication = cleanDuplication(data, null)
    val data_to_oneClick = cleanClickLowNum(data_to_duplication, 1) //.select("realip","realareacode","useragent","useros","userid","clientid","timestamps","timestamp_format","pagepath","ymd","fullurl","fullurlid","hostname","pagetitle","pagetitlecategoryid","pagetitlecategoryname","pagetitlekw","fullreferrrer","fullreferrerurl","organickeyword","source")
    saveHiveTable(sqlContext, data_to_oneClick, output01, true)
    //（2）取出类型ID为101003的记录。
    val data_to_oneClick2 = sqlContext.sql("select * from " + output01)
    val data_101003 = getAskData(data_to_oneClick2, fullurlid)
    saveHiveTable(sqlContext, data_101003, output02, true)
    //（3）过滤网址中带有“askSuccess”字样的网页记录。
    //（4）对咨询相关的数据中，网址存在的带“？”的网页网址进行处理，具体处理规则是以“？”为分割点，截取“？”前面的网址，数据存储在Hive表law_init1.data_101003_processed。
    val data_101003_2 = sqlContext.sql("select * from " + output02)
    val data_with_askSuccess = filterSpecialData(data_101003_2, fullurl, "askSuccess")
    val data_clean_wenhao = filterWenhao(data_with_askSuccess, sqlContext)
    saveHiveTable(sqlContext, data_clean_wenhao, output03, true)
    //（5）之前的重复数据处理主要是基于整行记录，现基于用户ID、网址、时间戳三个字段进行去重处理。
    //（6）去重后的数据可能存在了新的一次访问记录，需要在进行一次新的一次访问记录过滤操作，数据存储在law_init1.data_101003_url_gt_one。
    //（7）对数据按照用户ID、网址、时间戳三个字段进行去重处理，过滤出访问次数大于等于2的用户记录，数据存储在law_init1.data_101003_real_url_gt_one。
    val data_clean_wenhao2 = sqlContext.sql("select * from " + output03)
    val data_to_duplication_2 = cleanDuplication(data_clean_wenhao2, Array(userid, fullurl, timestamp_format))
    val data_to_oneClick_2 = cleanClickLowNum(data_to_duplication_2, 1)
    saveHiveTable(sqlContext, data_to_oneClick_2, output04, true)
    //（8）将用户ID、网页网址取出进行编码，编码完成后使用用户ID编码以及网址编码替换原先的用户ID和网页网址，数据存储在law_init1.data_101003_encoded。
    val data_to_oneClick3 = sqlContext.sql("select * from " + output04)
    val data_encode = encode_data(sqlContext, data_to_oneClick3, userCol, itemCol, newUserCol, newItemCol, userMetaTable, itemMetaTable)
    saveHiveTable(sqlContext, data_encode, output05, true)
  }
}
