package com.tipdm.dataclean;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;

public class SecondCleanDemo {
    public static void main(String[] args) {
        String mainClass = "com.tipdm.dataclean.SecondClean";
        String appName = "law_";
        String[] myArgs = {
                "law_init1.lawtime_gt_one_distinct",
                "law_init.data_101003_processed",
                "law_init.data_101003_url_gt_one",
                "law_init.data_101003_url_gt_one_distinct"
        };
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

