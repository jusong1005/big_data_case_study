package com.tipdm.java.jobs;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.java.JavaJobInterface;
import com.xxl.job.core.biz.model.ReturnT;

import java.util.Map;

public class BasisDataProcess implements JavaJobInterface{

    @Override
    public ReturnT<String> execute(String className, Map<String, String> args) throws Exception {
        String mainClass = "com.tipdm.chapter_2_6_1_basic_data_preprocess.BasicDataProcess";
        String appName = "basic data preprocess";
        String inputTable = args.get("--input");
        String outputTable = args.get("--output");
        String fullUrlColName = args.get("--fullUrlColName");
        String urlTypeColName = args.get("--urlTypeColName");
        String[] myArgs = new String[]{
                inputTable,
                outputTable,
                fullUrlColName,
                urlTypeColName,
                appName
        };
        SparkYarnJob.runAndMonitor(appName,mainClass,myArgs);
        return ReturnT.SUCCESS;
    }
}
