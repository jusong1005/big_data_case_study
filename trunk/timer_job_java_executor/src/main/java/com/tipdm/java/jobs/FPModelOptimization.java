package com.tipdm.java.jobs;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.java.JavaJobInterface;
import com.xxl.job.core.biz.model.ReturnT;

import java.util.Map;

public class FPModelOptimization implements JavaJobInterface{
    @Override
    public ReturnT<String> execute(String className, Map<String, String> args) throws Exception {
        String mainClass = "com.tipdm.chapter_2_8_3_fp_model_selection.FpModelSelection";
        String appName = "fp model selection ";
        String trainTable = args.get("--trainTable");//"law.law_visit_log_all_cleaned_101003_fp_train";
        String validateTable = args.get("--validateTable");//"law.law_visit_log_all_cleaned_101003_fp_validate";
        String transactionColName = args.get("--transactionColName");//"url_sets_column";
        String minSupports = args.get("--minSupports");
        String minConfidences = args.get("--minConfidences");
        String url = args.get("--mysqlurl");//"jdbc:mysql://node1:3306/law?characterEncoding=UTF-8";
        String table = args.get("--mysqltable");//"model_args";
        String user = args.get("--mysqluser");//"root";
        String password = args.get("--mysqlpwd");//"root";
        String[] myArgs = new String[]{
                trainTable,
                validateTable,
                transactionColName,
                minSupports,
                minConfidences,
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
