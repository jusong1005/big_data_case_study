package com.tipdm.dataclean;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.engine.type.EngineType;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;

public class ALSOptimizationTest {
    public static void main(String[] args) {
        String mainClass = "com.tipdm.optimization.ALSOptimization";
        String appName = "law_";
        String[] myArgs = new String[]{
                "law_init.data_101003_encoded_train",            //训练集
                "law_init.data_101003_encoded_validate",         //验证集
                "8,10",             //秩
                "8,10",             //迭代次数
                "0.03,0.09,0.1",   //lambda值
                "0.3,1.0,3.0",     //alpha值
                "true,false",      //是否调用ALS.trainImplicit构建模型
                "uid",              //用户ID列
                "pid",              //网页ID列
                "u_p_rate",         //用户评分
                "10,20,30",         //推荐个数
                "als_app",          //appName
                "law_init.lawtime_als_algorithm"              //参数存放表
        };
        Args innerArgs = Args.getArgs(appName,mainClass,myArgs, EngineType.SPARK);
        SubmitResult submitResult = SparkYarnJob.run(innerArgs);
        SparkYarnJob.monitor(submitResult);
    }
}
