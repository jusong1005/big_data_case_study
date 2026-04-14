package com.tipdm.model;

import com.tipdm.engine.SparkYarnJob;

public class FPModel {
    public static void main(String[] args) {
        String mainClass = "com.tipdm.model.FPModelTestV1";
        String appName = "law_";
        // 输入表，最小支持度，最小置信度
        String[] myArgs = new String[]{
                "law_init.lawtime_fp",
                "lawtime_fp_algorithm"
        };
//        String jobid = SparkEngine.submit(appName, mainClass, myArgs);
//        try {
//            SparkEngine.monitor(jobid);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        SparkYarnJob.runAndMonitor(appName,mainClass,myArgs);
    }
}