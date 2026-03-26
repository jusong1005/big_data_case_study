package com.tipdm.chapter_2_8_2_fp_model_creation;

import com.tipdm.engine.SparkYarnJob;

/**
 * 关联规则主题模型构造代码封装
 * @Author: fansy
 * @Time: 2019/1/14 15:16
 * @Email: fansy1990@foxmail.com
 */
public class FpModelCreationTest {
    public static void main(String[] args) {

        test();

    }

    public static void test(){
        String mainClass = "com.tipdm.chapter_2_8_2_fp_model_creation.FpModelCreation";
        String appName = "fp model creation ";
        String inputTable = "law.law_visit_log_all_cleaned_101003_train";
        String modelPath = "/tmp/fp_model_00";
        String modelTable = "law.law_visit_log_all_cleaned_101003_train_model";
        String transactionColName = "url_sets_column";
        String url = "jdbc:mysql://node1:3306/law?characterEncoding=UTF-8";
        String table = "model_args";
        String user = "root";
        String password="123456";
        String[] args = new String[]{
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
        SparkYarnJob.runAndMonitor(appName,mainClass,args);
    }
}
