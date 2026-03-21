package com.tipdm.dataclean;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.engine.type.EngineType;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;

public class FirstCleanDemo {
    public static void main(String[] args) {
        String mainClass = "com.tipdm.dataclean.FirstClean";
        String appName = "law_";
        String[] myArgs = {
                "law_init1.lawtime_all",
                "law_init.lawtime_gt_one",
                "law_init.lawtime_gt_one_distinct"
        };
//        String jobid = SparkEngine.submit(appName, mainClass, myArgs);
//        try {
//            SparkEngine.monitor(jobid);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

//        URL uri= FirstCleanDemo.class.getClassLoader().getResource("platform/");

        Args innerArgs = Args.getArgs(appName,mainClass,myArgs, EngineType.SPARK);
        SubmitResult submitResult = SparkYarnJob.run(innerArgs);
        SparkYarnJob.monitor(submitResult);

    }


}
