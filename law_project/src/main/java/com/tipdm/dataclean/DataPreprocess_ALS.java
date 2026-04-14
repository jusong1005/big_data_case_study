package com.tipdm.dataclean;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.engine.type.EngineType;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;

public class DataPreprocess_ALS {
    public static void main(String[] args) {
        String mainClass = "com.tipdm.dataclean.DataPreprocess02_ALS";
        String appName = "law_";
        String[] myArgs = new String[]{
                "law_init.lawtime_encode",
                "law_init.data_101003_encode_als",
                "uid",
                "pid",
                "rating"
        };
//        String jobid = SparkEngine.submit(appName, mainClass, myArgs);
//        try {
//            SparkEngine.monitor(jobid);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        // 构造参数
//        Args innerArgs = Args.getArgs(appName,mainClass,myArgs); // 默认引擎
        Args innerArgs = Args.getArgs(appName,mainClass,myArgs, EngineType.SPARK); // 动态指定引擎
        // 运行算法
        SubmitResult submitResult = SparkYarnJob.run(innerArgs);
        // 进行监控
        SparkYarnJob.monitor(submitResult);
    }
}
