package com.tipdm.dataclean;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;

public class RecommendSaveTest {
    public static void main(String[] args) {
        String mainClass = "com.tipdm.recommendsave.RecommendSave";
        String appName = "law_";
        String[] myArgs = new String[5];
        myArgs[0] = "recommendSave hbase";
        myArgs[1] = "law_recommend";
        myArgs[2] = "recommends";
        myArgs[3] = "rec";
        myArgs[4] = "/user/root/fpRecommendPath";

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
