package com.tipdm.chapter_2_6_2_als_data_preprocess;
import com.tipdm.engine.SparkYarnJob;
/**
 * 测试 ALS模型主题数据预处理模块
 */
public class DataProcessBasedOnAlsTest {
    public static void main(String[] args) {
        boolean output_model_data = true; // 测试 建模输出数据
//        boolean output_model_data = false; // 测试 模型参数择优输出数据
        test(output_model_data);
    }
    public static void test(boolean output_model_data_boolean){
        String mainClass = "com.tipdm.chapter_2_6_2_als_data_preprocess.DataProcessBasedOnAls";
        String appName = "als data preprocess ";
        String output_model_data = String.valueOf(output_model_data_boolean);
        String inputTable = "law.law_visit_log_all_cleaned_101003";
        String train_validate_percent = "0.8,0.2";
        String modelTrainTable = "law.law_visit_log_all_cleaned_als_train";
        String validateTrainTable = "law.law_visit_log_all_cleaned_als_validate_train";
        String validateTable = "law.law_visit_log_all_cleaned_als_validate";
        String userMetaTable = "law.law_visit_log_all_userMeta";
        String urlMetaTable = "law.law_visit_log_all_urlMeta";
        String userColName = "userid";
        String urlColName = "fullurl";
        String timeColName = "timestamp_format";
        String useridColName = "uid";
        String urlidColName = "pid";

        String[] args = new String[]{
                output_model_data,
                inputTable,
                train_validate_percent,
                modelTrainTable,
                validateTrainTable,
                validateTable,
                userMetaTable,
                urlMetaTable,
                userColName,
                urlColName,
                timeColName,
                useridColName,
                urlidColName,
                appName
        };
        SparkYarnJob.runAndMonitor(appName,mainClass,args);
    }
}
