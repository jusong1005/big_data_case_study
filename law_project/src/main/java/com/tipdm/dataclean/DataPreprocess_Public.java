package com.tipdm.dataclean;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;

public class DataPreprocess_Public {
    public static void main(String[] args) {
        String mainClass = "com.tipdm.dataclean.DataPreprocess01_Public";
        String appName = "law_";
        String[] myArgs = new String[12];
        myArgs[0] = "law_init1.lawtime_all";
        myArgs[1] = "law_init.lawtime_gt_one_distinct";
        myArgs[2] = "law_init.data_101003";
        myArgs[3] = "law_init.data_101003_processed";
        myArgs[4] = "law_init.data_101003_url_gt_one_distinct";
        myArgs[5] = "userid";
        myArgs[6] = "fullurl";
        myArgs[7] = "uid";
        myArgs[8] = "pid";
        myArgs[9] = "law_init.data_101003_usermeta";
        myArgs[10] = "law_init.data_101003_itemmeta";
        myArgs[11] = "law_init.lawtime_encode";
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
