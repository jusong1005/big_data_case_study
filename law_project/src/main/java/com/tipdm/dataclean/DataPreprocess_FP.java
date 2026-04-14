package com.tipdm.dataclean;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;

public class DataPreprocess_FP {
    public static void main(String[] args) {
        String mainClass = "com.tipdm.dataclean.DataPreprocess03_FP";
        String appName = "law_";
        String[] myArgs = new String[5];
        myArgs[0] = "law_init.lawtime_encode";
        myArgs[1] = "uid";
        myArgs[2] = "pid";
        myArgs[3] = "URL_SETS_COLUMN";
        myArgs[4] = "law_init.lawtime_fp";
//        String jobid = SparkEngine.submit(appName, mainClass, myArgs);
//        try {
//            SparkEngine.monitor(jobid);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        Args innerArgs = Args.getArgs(appName,mainClass,myArgs);
        SubmitResult submitResult = SparkYarnJob.run(innerArgs);
        SparkYarnJob.monitor(submitResult);
    }
}
