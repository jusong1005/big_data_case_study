package com.tipdm.dataclean;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;

public class RecommendSave_MYSQL {
    public static void main(String[] args) {
        String mainClass = "com.tipdm.recommendsave.RecommendSave_MYSQL";
        String appName = "law_";
        String[] myArgs = new String[7];
        myArgs[0] = "recommendSave mysql";
        myArgs[1] = "/user/root/fpRecommendPath";
        myArgs[2] = "law_recommend";
        myArgs[3] = "law_init";
        myArgs[4] = "192.168.2.162";
        myArgs[5] = "10";
        myArgs[6] = "userid,rec1,rec2,rec3,rec4,rec5,rec6,rec7,rec8,rec9,rec10";


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
