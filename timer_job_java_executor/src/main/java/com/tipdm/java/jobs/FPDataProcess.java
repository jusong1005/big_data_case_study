package com.tipdm.java.jobs;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.java.JavaJobInterface;
import com.xxl.job.core.biz.model.ReturnT;

import java.util.Map;

/**
 * 测试 关联规则主题数据预处理模块
 * @Author: fansy
 * @Time: 2019/1/12 13:50
 * @Email: fansy1990@foxmail.com
 */
public class FPDataProcess implements JavaJobInterface{

    @Override
    public ReturnT<String> execute(String className, Map<String, String> args) throws Exception {
        String mainClass = "com.tipdm.chapter_2_6_3_fp_data_preprocess.DataProcessBasedOnFp";
        String appName = "fp data preprocess  ";
        String output_model_data = args.get("--ismodeldata");
        String inputTable = args.get("--inputTable");
        String train_validate_percent = args.get("--train_validate_percent");
        String modelTrainTable = args.get("--modelTrainTable");
        String validateTrainTable = args.get("--validateTrainTable");
        String validateTable = args.get("--validateTable");
        String uidColName = args.get("--uidColName");
        String urlColName = args.get("--urlColName");
        String timeColName = args.get("--timeColName");
        String[] myArgs = new String[]{
                output_model_data,
                inputTable,
                train_validate_percent,
                modelTrainTable,
                validateTrainTable,
                validateTable,
                uidColName,
                urlColName,
                timeColName,
                appName
        };
        SparkYarnJob.runAndMonitor(appName,mainClass,myArgs);
        return ReturnT.SUCCESS;
    }
}
