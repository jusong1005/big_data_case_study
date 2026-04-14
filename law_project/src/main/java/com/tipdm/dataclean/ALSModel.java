package com.tipdm.dataclean;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.engine.type.EngineType;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;

public class ALSModel {
    public static void main(String[] args) {
        String mainClass = "com.tipdm.model.ALSModelTest";
        String appName = "law_";
        String[] myArgs = new String[8];
        myArgs[0] = "law_init.data_101003_encoded_train";     // 训练集
        myArgs[1] = "law_init.lawtime_als_algorithm";         // 输入参数表
        myArgs[2] = "/user/root/ALSModelPath";                 // 模型存储
        myArgs[3] = "bestrank";                                  // 参数名称
        myArgs[4] = "bestiter";
        myArgs[5] = "bestalpha";
        myArgs[6] = "bestreg";
        myArgs[7] = "bestimplicitprefs";

        Args innerArgs = Args.getArgs(appName,mainClass,myArgs, EngineType.SPARK);
        SubmitResult submitResult = SparkYarnJob.run(innerArgs);
        SparkYarnJob.monitor(submitResult);

    }
}
