package com.tipdm.dataclean;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;

public class FPModelCreate {
    public static void main(String[] args) {
        String mainClass = "com.tipdm.model.FPModelTestV1";
        String appName = "law_";
        String[] myArgs = new String[5];
        myArgs[0] = "law_init.data_101003_url_gt_one_distinct";
        myArgs[1] = "law_init.lawtime_fp_algorithm";
        myArgs[2] = "userid";
        myArgs[3] = "fullurl";
        myArgs[4] = "law_init.data_101003_fp_rules";
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
