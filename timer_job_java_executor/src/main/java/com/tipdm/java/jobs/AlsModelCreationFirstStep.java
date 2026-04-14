package com.tipdm.java.jobs;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.java.JavaJobInterface;
import com.tipdm.utils.SparkUtils;
import com.xxl.job.core.biz.model.ReturnT;
import org.apache.hadoop.fs.Path;

import java.util.Map;

/**
 * 构建ALS模型
 * ring
 */
public class AlsModelCreationFirstStep implements JavaJobInterface{
    @Override
    public ReturnT<String> execute(String className, Map<String, String> args) throws Exception {
        String mainClass = "com.tipdm.chapter_2_7_2_als_model_creation_recommend.AlsModelCreationFirstStep";
        String appName = "als model creation first step ";
        String inputTable = args.get("--inputTable");//"law.law_visit_log_all_cleaned_als_train";
        String useridCol = args.get("--useridCol");//"uid";
        String urlidCol = args.get("--urlidCol");//"pid";
        String ratingCol = args.get("--ratingCol");//"rating";
        String url = args.get("--mysqlurl");//"jdbc:mysql://node1:3306/law?characterEncoding=UTF-8";
        String table = args.get("--mysqltable");//"als_model_args";
        String user = args.get("--mysqluser");//"root";
        String password = args.get("--mysqlpwd");//"root";
        String modelTable = args.get("--modelTable");//"/tmp/spark/als_split_model";
        String[] myArgs = new String[]{
                inputTable,
                useridCol,
                urlidCol,
                ratingCol,
                url,
                table,
                user,
                password,
                modelTable,
                appName
        };
        SparkUtils.getFs().delete(new Path(modelTable),true);
        SparkYarnJob.runAndMonitor(appName,mainClass,myArgs);
        return ReturnT.SUCCESS;
    }
}
