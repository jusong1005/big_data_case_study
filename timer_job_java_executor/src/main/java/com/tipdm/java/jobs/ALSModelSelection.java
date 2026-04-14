package com.tipdm.java.jobs;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.java.JavaJobInterface;
import com.xxl.job.core.biz.model.ReturnT;

import java.util.Map;

public class ALSModelSelection implements JavaJobInterface {

    @Override
    public ReturnT<String> execute(String className, Map<String, String> args) throws Exception {
        String mainClass = "com.tipdm.chapter_2_7_2_als_model_selection.AlsModelSelection";
        String appName = "als model selection ";
        String trainTable = args.get("--trainTable");//"law.law_visit_log_all_cleaned_als_validate_train";//训练集
        String validateTable = args.get("--validateTable");//"law.law_visit_log_all_cleaned_als_validate";//验证集
        String ranks = args.get("--ranks");//"8,10,12"; //秩
        String iterations = args.get("--iterations");//"8,10,12"; //迭代次数
        String regs = args.get("--regs");//"0.09,0.1,0.3"; //lambda值
        String alphas = args.get("--alphas");//"0.3,1.0,3.0";  //alpha值
        String implicitPrefs = args.get("--implicitPrefs");//"true,false";  //是否调用ALS.trainImplicit构建模型
        String userCol = args.get("--userCol");//"uid";   //用户ID列
        String itemCol = args.get("--itemCol");//"pid";   //网页ID列
        String ratingCol = args.get("--ratingCol");//"rating";  //用户评分
        String url = args.get("--sqlurl");//jdbc:mysql://node1:3306/law?characterEncoding=UTF-8";
        String table = args.get("--sqltable");//"alsmodel_args";
        String user = args.get("--sqluser");//"root";
        String password = args.get("--sqlpwd");//"root";
//            String bestModelPath = "law.lawtime_als_algorithm";  //参数存放hive表
        String recNums = args.get("--recNums");//"10,20,30";  //推荐个数
        String[] myArgs = new String[]{
                trainTable,
                validateTable,
                ranks,
                iterations,
                regs,
                alphas,
                implicitPrefs,
                userCol,
                itemCol,
                ratingCol,
//                    bestModelPath,
                url,
                table,
                user,
                password,
                recNums,
                appName
        };
        SparkYarnJob.runAndMonitor(appName,mainClass,myArgs);
        return ReturnT.SUCCESS;
    }
}
