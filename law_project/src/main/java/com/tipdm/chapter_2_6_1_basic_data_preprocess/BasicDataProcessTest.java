package com.tipdm.chapter_2_6_1_basic_data_preprocess;

import com.tipdm.engine.SparkYarnJob;

/**
 * 基础数据预处理测试
 * @Author: fansy
 * @Time: 2019/2/12 14:58
 * @Email: fansy1990@foxmail.com
 */
public class BasicDataProcessTest {
    public static void main(String[] args) {
        test();
    }
    public static void test(){
        String mainClass = "com.tipdm.chapter_2_6_1_basic_data_preprocess.BasicDataProcess";
        String appName = "basic data preprocess";
        String inputTable = "law.law_visit_log_all";
        String outputTable = "law.law_visit_log_all_basic_processed";
        String fullUrlColName = "fullurl";
        String urlTypeColName = "fullurlid";
        String[] args = new String[]{
                inputTable,
                outputTable,
                fullUrlColName,
                urlTypeColName,
                appName
        };
        SparkYarnJob.runAndMonitor(appName,mainClass,args);
    }
}
