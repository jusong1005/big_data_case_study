package com.tipdm.dataclean;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;

public class DataPreprocess_Optimzation {
    public static void main(String[] args) {
        String mainClass = "com.tipdm.dataclean.DataPreprocess04_Optimization";
        String appName = "law_";
        String[] myArgs = new String[7];
        myArgs[0] = "law_init.lawtime_encode";
        myArgs[1] = "0.8";
        myArgs[2] = "0.1";
        myArgs[3] = "0.1";
        myArgs[4] = "law_init.data_101003_encoded_train";
        myArgs[5] = "law_init.data_101003_encoded_validate";
        myArgs[6] = "law_init.data_101003_encoded_test";

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
