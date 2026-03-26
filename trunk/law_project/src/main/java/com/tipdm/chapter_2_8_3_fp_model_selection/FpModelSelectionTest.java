package com.tipdm.chapter_2_8_3_fp_model_selection;

import com.tipdm.engine.SparkYarnJob;

/**
 * 2.8.3	FP 寻优过程及参数封装 测试
 * @Author: fansy
 * @Time: 2019/1/17 14:49
 * @Email: fansy1990@foxmail.com
 */
public class FpModelSelectionTest {
    public static void main(String[] args) {
        test();
    }
    public static void test(){
        try {
            Thread.sleep(1000 * 5600L);// 休眠1小时
        }catch (Exception e){
            e.printStackTrace();
        }
        String mainClass = "com.tipdm.chapter_2_8_3_fp_model_selection.FpModelSelection";
        String appName = "fp model selection ";
        String trainTable = "law.law_visit_log_cleaned_101003_fp_train";
        String validateTable = "law.law_visit_log_cleaned_101003_fp_validate";
        String transactionColName = "url_sets_column";
     //   String minSupports = "0.000006,0.000007,0.000008,0.000009,0.00001,0.00002,0.00003";
        String minSupports = "0.0003,0.003";
        String minConfidences = "0.03,0.06,0.1,0.2,0.3";
        String url = "jdbc:mysql://node1:3306/law?characterEncoding=UTF-8";
        String table = "model_args";
        String user = "root";
        String password="123456";
        String[] args = new String[]{
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
        SparkYarnJob.runAndMonitor(appName,mainClass,args);
    }
}
