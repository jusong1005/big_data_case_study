package com.tipdm.util;

import com.tipdm.engine.SparkYarnJob;

public class CommonUtilDemo {

    public static void main(String[] args) {
        String mainClass = "com.tipdm.dataclean.FirstClean";
        String[] myArgs = {
                "hdfs://192.168.0.181:8020/opt/guoxiang/wordcount.txt",
                "hdfs://192.168.0.181:8020/opt/guoxiang/wordcount"
        };
        String appName = "law_";
//        SparkEngine.submit(appName, mainClass, myArgs);
        SparkYarnJob.runAndMonitor(appName,mainClass,myArgs);
    }
}
