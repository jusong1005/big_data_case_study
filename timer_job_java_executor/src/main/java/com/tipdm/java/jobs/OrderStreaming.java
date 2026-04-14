package com.tipdm.java.jobs;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.java.JavaJobInterface;
import com.xxl.job.core.biz.model.ReturnT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * //@Author:qwm
 * //@Date: 2018/11/14 9:18
 */
public class OrderStreaming implements JavaJobInterface {
    private static Logger log = LoggerFactory.getLogger(OrderStreaming.class);
    private static String  sparkClassName="com.tipdm.scala.streaming.KafkaStream";
    private static  String applicationName = "KafkaStream";
    public static void main(String[] args) throws Exception {
        String className = "com.tipdm.java.jobs.OrderStreaming";
        OrderStreaming orderStreaming = new OrderStreaming();
        orderStreaming.execute(className,null);
    }


    @Override
    public ReturnT<String> execute(String jobClassName, Map<String, String> args) throws Exception {
        String[] arguments =new String[1];
        arguments[0]="test";
//        String applicationId = SparkEngine.submit(applicationName,sparkClassName,arguments);
//        SparkEngine.monitor(applicationId);
        SparkYarnJob.runAndMonitor(applicationName,sparkClassName,arguments);
        log.info("任务运行成功");
        return ReturnT.SUCCESS;
    }
}
