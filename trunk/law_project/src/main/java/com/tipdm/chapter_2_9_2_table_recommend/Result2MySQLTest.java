package com.tipdm.chapter_2_9_2_table_recommend;

import com.tipdm.engine.SparkYarnJob;

/**
 * 表推荐结果写入MySQL测试
 * @Author: fansy
 * @Time: 2019/2/15 16:38
 * @Email: fansy1990@foxmail.com
 */
public class Result2MySQLTest {
    public static void main(String[] args) {
        test();
    }
    public static void test(){
        String mainClass = "com.tipdm.chapter_2_9_2_table_recommend.Result2MySQL";
        String appName = "Als Recommend write to MySQL ";
        String inputTableDB = "law";
        String inputTablePrefix = "law_visit_log_all_als_model_m";
        String url = "jdbc:mysql://node1:3306/law?characterEncoding=UTF-8";
        String table = "law_visit_log_als_recommends";
        String user = "root";
        String password = "root";
        String[] args = new String[]{
                inputTableDB,
                inputTablePrefix,
                url,
                table,
                user,
                password,
                appName
        };
        SparkYarnJob.runAndMonitor(appName,mainClass,args);
    }
}
