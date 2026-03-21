package com.tipdm.dataclean;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;

public class FPOptimization {
    public static void main(String[] args) {
        String mainClass = "com.tipdm.optimization.FPOptimization";   //调用的Scala类
        String appName="law_";
        String[] myArgs = new String[]{
                "law_init.data_101003_encoded_train",       //训练集
                "law_init.data_101003_encoded_validate",   //验证集
                "uid",                                           //用户id
                "pid",                                           //网页id
                "0.00001,0.00003",                             //最小支持度
                "0.001,0.003",                                  //最小置信度
                "10,20",                                         //推荐个数
                "fp_app",                                        //appName
                "law_init.lawtime_fp_algorithm"              //参数存放路径
        };
//        String jobid= SparkEngine.submit(appName, mainClass, myArgs);
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
