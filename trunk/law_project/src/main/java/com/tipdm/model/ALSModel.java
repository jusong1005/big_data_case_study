package com.tipdm.model;


import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;

public class ALSModel {
    public static void main(String[] args) {
        String mainClass = "com.tipdm.model.ALSModelTest";
        String appName = "law_";
        // 输入表，因子数量，迭代次数，正则化参数，基线置信度，显式/隐式反馈
        String[] myArgs = new String[2];
        myArgs[0] = "law_init.data_101003_encode_als";
        myArgs[1] = "law_init.lawtime_als_algorithm";
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

