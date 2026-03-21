package com.tipdm.java.chapter_3_4_3_datasource;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;

/**
 * //@Author:qwm
 * //@Date: 2018/09/17 13:42
 *
 */

public class Hive2Elasticsearch {
    private static String className = "com.tipdm.scala.chatper_3_5_1_datasource.Hive2Elasticsearch";
    private static String applicationName = "Hive2Elasticsearch";

    public static void main(String[] args) throws Exception {
        String[] arguments = new String[6];
        arguments[0] = "media_1d";
        arguments[1] = "192.168.111.75";
        arguments[2] = "9200";
        arguments[3] = "origin_time";
        arguments[4] = "media_index";
        arguments[5] = "media";
//        String applicationId = SparkEngine.submit(applicationName, className, arguments);
//        SparkEngine.monitor(applicationId);
        Args innerArgs = Args.getArgs(applicationName,className,arguments);
        SubmitResult submitResult = SparkYarnJob.run(innerArgs);
        SparkYarnJob.monitor(submitResult);

        System.out.println("任务运行成功");
    }
}
