package com.tipdm.chapter_2_6_3_fp_data_preprocess;

import com.tipdm.engine.SparkYarnJob;

/**
 * 测试 关联规则主题数据预处理模块
 * @Author: fansy
 * @Time: 2019/1/12 13:50
 * @Email: fansy1990@foxmail.com
 */
public class DataProcessBasedOnFpTest {
    public static void main(String[] args) {

//        boolean output_model_data = true; // 测试 建模输出数据
        boolean output_model_data = false; // 测试 模型参数择优输出数据
        test(output_model_data);

    }

    public static void test(boolean output_model_data_boolean){
        String mainClass = "com.tipdm.chapter_2_6_3_fp_data_preprocess.DataProcessBasedOnFp";
        String appName = "fp data preprocess  ";
        String output_model_data = String.valueOf(output_model_data_boolean);
        String inputTable = "law.law_visit_log_all_cleaned_101003";
        String train_validate_percent = "0.8,0.2";
        String modelTrainTable = "law.law_visit_log_all_cleaned_101003_train";
        String validateTrainTable = "law.law_visit_log_all_cleaned_101003_fp_train";
        String validateTable = "law.law_visit_log_all_cleaned_101003_fp_validate";
        String uidColName = "userid";
        String urlColName = "fullurl";
        String timeColName = "timestamp_format";
        String[] args = new String[]{
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
        SparkYarnJob.runAndMonitor(appName,mainClass,args);
    }
}
