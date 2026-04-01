package com.tipdm.scala.chapter_3_7_streaming

import java.io.{BufferedInputStream, InputStream}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import com.tipdm.scala.util.InternalRedisClient
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Accumulator, SparkConf}
import redis.clients.jedis.Pipeline

/**
  * Created by ch on 2018/8/15
  *
  *SparkStreaming统计订单的信息
  */
object KafkaStream {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("kafkaStream")
    // 相地模式运行可使用conf.setMaster("local[*]")
//     conf.setMaster("local[*]")
    // 窗口时间设置为30分钟
    //val ssc = new StreamingContext(conf, Seconds(60*30))
    val ssc = new StreamingContext(conf, Seconds(10))
    val sqlContext = new SQLContext(ssc.sparkContext)
    val properties: Properties = new Properties()
    val inputStream: InputStream = getClass.getResourceAsStream("/sysconfig/kafka.properties")
    properties.load(new BufferedInputStream(inputStream))
    // kafka topic
    val topic = properties.getProperty("kafka.topics")
    // kafka的分区数
    val partitions = properties.getProperty("kafka.num.partitions").toInt
    val topics = Set(topic)
    val brokers = properties.getProperty("kafka.brokers")
    val kafkaParams: Map[String, String] = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> "exactly-once",
      "enable.auto.commit" -> "false"
    )
    inputStream.close()
    // 从redis读取主题各分区的offerset
    val jedis = InternalRedisClient.getJedis()
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    for (i <- 0 until partitions) {
      var offerSet = 0l
      val tmp = jedis.get(topic + "_" + i)
      if (null != tmp) {
        offerSet = tmp.toLong
      }
      fromOffsets += (new TopicAndPartition(topic, i) -> offerSet)
    }
    // 定义订单总金额
    val totalCost: Accumulator[Double] = ssc.sparkContext.accumulator(0.0)
    // 定义有效订单总数
    val validOrders: Accumulator[Int] = ssc.sparkContext.accumulator(0)
    val historyTotal = jedis.get("totalcost")
    val historyValidOrders = jedis.get("validOrders")
    val historyOrders = jedis.get("totalOrders")
    jedis.close()
    // 定义新增有效订单数
    val increaseValidOrders = ssc.sparkContext.accumulator(0)
    // 定义新增订单金额
    val increaseCost: Accumulator[Double] = ssc.sparkContext.accumulator(0.0)
    var sum: Long = 0
    if (null != historyOrders) {
      sum = historyOrders.toInt
    }
    if (null != historyValidOrders) {
      validOrders.add(historyValidOrders.toInt)
    }
    if (null != historyTotal) {
      totalCost.add(historyTotal.toDouble)
    }

    val messageHandler: MessageAndMetadata[String, String] => (String, String) =
      (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())
    val offsetRanges = Array[OffsetRange]()
    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    // 在这里处理每一批过来的数据
    kafkaStream.foreachRDD(
      rdd => {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        // 新增订单每一批次重置为0
        increaseValidOrders.setValue(0)
        // 新增订单营业额重置为0
        increaseCost.setValue(0.0)
        // 按业务逻辑处理每一行数据
        rdd.foreach(row=>{
          val fields = row._2.split(";")
          val cost = fields(14)
          // 订单有效判断
          if (null == cost || cost.equalsIgnoreCase("null") || cost.startsWith("YH")) {
          } else {
            // 有效订单统计
            validOrders.add(1)
            increaseValidOrders.add(1)
            // 每次总营业额
            increaseCost.add(cost.toDouble)
            // 总营业额
            totalCost.add(cost.toDouble)
          }
        })
        // 记录总数即订单总数
        sum = sum + rdd.count()
        println("新增订单数: " + rdd.count())
        println("新增有效订单数：" + increaseValidOrders)
        println("有效订单总数： " + validOrders)
        println("总订单数: " + sum)
        println("新增订单营业额：" + increaseCost)
        println("订单总营业额：" + totalCost)
        val jedis = InternalRedisClient.getJedis()
        val pipeLine: Pipeline = jedis.pipelined()
        pipeLine.multi() // 开启事务
        pipeLine.set("totalcost", totalCost.toString())
        // 每小时统计一次总营业额，总订单，有效订单保存到redis,key以时间开头(如2018082413_xxx)
        val key = nowTime()
        if (key.endsWith("00")) {
          val totalKey = key.substring(0, 10) + "_totalcost"
          pipeLine.set(totalKey, totalCost.toString())
          pipeLine.set(key.substring(0, 10) + "_totalorders", sum.toString)
          pipeLine.set(key.substring(0, 10) + "_validorders", validOrders.toString())
        }
        // 每一批次都更新统计指标
        pipeLine.set("increase_cost", increaseCost.toString())
        pipeLine.set("increase_valid_order", increaseValidOrders.toString)
        pipeLine.set("totalValidOrders", validOrders.toString())
        pipeLine.set("totalOrders", sum.toString)
        pipeLine.set("increase_order", rdd.count().toString)
        // 保存spark streaming消费kafka的topic的partition中的offset，以便重启后继续从上次的置消费
        offsetRanges.foreach { offsetRange =>
          println("partition : " + offsetRange.partition + " fromOffset:  " + offsetRange.fromOffset + " untilOffset: " + offsetRange.untilOffset)
          val topic_partition_key = offsetRange.topic + "_" + offsetRange.partition
          pipeLine.set(topic_partition_key, offsetRange.untilOffset + "")
        }
        pipeLine.exec(); // 提交事务
        pipeLine.sync(); // 关闭pipeline
        jedis.close()
      }
    )
    ssc.start()
    ssc.awaitTermination()
  }
  /**
    * 获取当前时间，返回的格式：yyyyMMddHHmm
    * @return
    */
  def nowTime(): String = {
    val now: Date = new Date()
    val dataFormate: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm")
    val date = dataFormate.format(now)
    return date
  }
}
