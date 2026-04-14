package com.tipdm.chapter_2_9_2_table_recommend

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf

/**
  * 推荐结果写入HBase
  * //@Author: fansy 
  * //@Time: 2019/2/15 16:56
  * //@Email: fansy1990@foxmail.com
  */
object Result2HBase {

  def main(args: Array[String]): Unit = {
    if (args.length != 9) {
      printUsage()
      System.exit(1)
    }
    //  0. 参数处理
    val (inputTableDB, inputTablePrefix, zookeeper_quorum, zookeeper_clientPort, table, cf, url_qualifier, rating_qualifier, appName) = handle_args(args)

    //  1. 初始化
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    // 2. 拼接数据表
    val specificTables = sqlContext.sql("SHOW TABLES IN " + inputTableDB)
      .filter("tableName like '" + inputTablePrefix + "%'").select("tableName")
      .rdd.map(row => row.getString(0)).collect()

    // 3. 整合数据
    val data = specificTables.map(t => sqlContext.read.table(inputTableDB + "." + t)).reduce((x, y) => x.unionAll(y))

    // 4. 转换数据
    val result = data.rdd.map(row => (row.getString(0), row.getString(1), row.getDouble(2)))
      .map { x =>
        val rowkey = Bytes.toBytes(x._1)
        val key = new ImmutableBytesWritable(rowkey)

        val put = new Put(rowkey)
        val currTs = System.currentTimeMillis()
        put.addColumn(cf, url_qualifier, currTs, Bytes.toBytes(x._2))
        put.addColumn(cf, rating_qualifier, currTs, Bytes.toBytes(x._3.toString))
        (key, put)
      }

    // 5. 获取HBase配置
    val hbaseConf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置  
    hbaseConf.set("hbase.zookeeper.quorum", zookeeper_quorum)
    //设置zookeeper连接端口，默认2181  
    hbaseConf.set("hbase.zookeeper.property.clientPort", zookeeper_clientPort)

    // 6. 清空HBase表
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val admin = connection.getAdmin()
    admin.disableTable(TableName.valueOf(table))
    admin.truncateTable(TableName.valueOf(table), false)
    admin.close()

    // 7. 写入HBase
    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, table)

    result.saveAsHadoopDataset(jobConf)

    // 8. 关闭SparkContext
    sc.stop()

  }

  def printUsage() : Unit = {
    val buff = new StringBuilder
    buff.append("Usage : com.tipdm.chapter_2_9_2_table_recommend.Result2HBase").append(" ")
      .append("<inputTableDB>").append(" ")
      .append("<inputTablePrefix>").append(" ")
      .append("<zookeeper_quorum>").append(" ")
      .append("<zookeeper_clientPort>").append(" ")
      .append("<table>").append(" ")
      .append("<cf>").append(" ")
      .append("<url_qualifier>").append(" ")
      .append("<rating_qualifier>").append(" ")
      .append("<appName>").append(" ")
    println(buff.toString())
  }

  def handle_args(args: Array[String]) = {
    val inputTableDB = args(0)
    val inputTablePrefix = args(1)
    val zookeeper_quorum = args(2)
    val zookeeper_clientPort = args(3)
    val table = args(4)
    val cf = Bytes.toBytes(args(5))
    val url_qualifier = Bytes.toBytes(args(6))
    val rating_qualifier = Bytes.toBytes(args(7))
    val appName = args(7)
    (inputTableDB, inputTablePrefix, zookeeper_quorum, zookeeper_clientPort, table, cf, url_qualifier, rating_qualifier, appName)
  }

}