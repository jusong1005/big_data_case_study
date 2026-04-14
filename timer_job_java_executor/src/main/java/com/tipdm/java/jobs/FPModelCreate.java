package com.tipdm.java.jobs;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.java.JavaJobInterface;
import com.xxl.job.core.biz.model.ReturnT;

import java.util.Map;

public class FPModelCreate implements JavaJobInterface{

    @Override
    public ReturnT<String> execute(String className, Map<String, String> args) throws Exception {
        String mainClass = "com.tipdm.chapter_2_8_2_fp_model_creation.FpModelCreation";
        String appName = "fp model creation ";
        String inputTable = args.get("--inputTable");//"law.law_visit_log_all_cleaned_101003_train";
        String modelPath = args.get("--modelPath");//"/tmp/fp_model_00";
        String modelTable = args.get("--modelTable");//"law.law_visit_log_all_cleaned_101003_train_model";
        String transactionColName = args.get("--transactionColName");//"url_sets_column";
        String url = args.get("--mysqlurl");//jdbc:mysql://node1:3306/law?characterEncoding=UTF-8";
        String table = args.get("--mysqltable");//"model_args";
        String user = args.get("--mysqluser");//"root";
        String password = args.get("--mysqlpwd");//"root";
        String[] myArgs = new String[]{
                inputTable,
                modelPath,
                modelTable,
                transactionColName,
                url,
                table,
                user,
                password,
                appName
        };
        SparkYarnJob.runAndMonitor(appName,mainClass,myArgs);
        return ReturnT.SUCCESS;
    }
}
