package com.tipdm.java.chapter_3_7_streaming;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by ch on 2018/11/8
 *
 * 订单流的调用
 */
public class OrderStreaming {
    private static final Logger logger = LoggerFactory.getLogger(OrderStreaming.class);
    private static String className = "com.tipdm.scala.chapter_3_7_streaming.KafkaStream";
    private static String applicationName = "Order Spark Streaming";

    public static void main(String[] args) throws Exception {
        String[] arguments = new String[1];
        arguments[0] = "test";
        Args innerArgs = Args.getArgs(applicationName,className,arguments);
        SubmitResult submitResult = SparkYarnJob.run(innerArgs);
        SparkYarnJob.monitor(submitResult);
//        String applicationId = SparkEngine.submit(applicationName, className, arguments);
//        SparkEngine.monitor(applicationId);
        logger.info("运行成功");

    }
}
