package com.tipdm.dataclean;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;

public class FPModelRecommend {
    public static void main(String[] args) {
        String mainClass = "com.tipdm.model.FPModelRecommend";
        String appName = "law_";
        String[] myArgs = new String[6];
        myArgs[0] = "law_init.data_101003_url_gt_one_distinct";
        myArgs[1] = "law_init.data_101003_fp_rules";
        myArgs[2] = "10";
        myArgs[3] = "userid";
        myArgs[4] = "fullurl";
        myArgs[5] = "/user/root/recommendPath";
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
