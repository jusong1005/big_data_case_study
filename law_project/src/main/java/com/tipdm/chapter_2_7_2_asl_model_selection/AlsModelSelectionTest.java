package com.tipdm.chapter_2_7_2_asl_model_selection;


import com.tipdm.engine.SparkYarnJob;

    /**
     * 2.7.2ALS 寻优过程及参数封装 测试
     */
    public class AlsModelSelectionTest {
        public static void main(String[] args) {
            test();
        }
        public static void test(){
            String mainClass = "com.tipdm.chapter_2_7_2_als_model_selection.AlsModelSelection";
            String appName = "als model selection ";
            String trainTable = "law.law_visit_log_all_cleaned_als_validate_train";//训练集
            String validateTable = "law.law_visit_log_all_cleaned_als_validate";//验证集
            String ranks = "10"; //秩
            String iterations = "8"; //迭代次数
            String regs = "0.09"; //lambda值
            String alphas = "1.0";  //alpha值
            String implicitPrefs = "true,false";  //是否调用ALS.trainImplicit构建模型
            String userCol = "uid";   //用户ID列
            String itemCol = "pid";   //网页ID列
            String ratingCol = "rating";  //用户评分
            String url = "jdbc:mysql://node1:3306/law?characterEncoding=UTF-8";
            String table = "als_model_args";
            String user = "root";
            String password="123456";
//            String bestModelPath = "law.lawtime_als_algorithm";  //参数存放hive表
            String recNums = "10";  //推荐个数
            String[] args = new String[]{
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
            SparkYarnJob.runAndMonitor(appName,mainClass,args);
        }
    }

