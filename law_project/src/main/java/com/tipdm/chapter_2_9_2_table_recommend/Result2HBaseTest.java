package com.tipdm.chapter_2_9_2_table_recommend;

import com.tipdm.engine.SparkYarnJob;

/**
 * 表推荐结果写入HBase测试
 * @Author: fansy
 * @Time: 2019/2/15 16:38
 * @Email: fansy1990@foxmail.com
 */
public class Result2HBaseTest {
    public static void main(String[] args) {
        test();
    }
    public static void test(){
        String mainClass = "com.tipdm.chapter_2_9_2_table_recommend.Result2HBase";
        String appName = "Als Recommend write to HBase ";
        String inputTableDB = "law";
        String inputTablePrefix = "law_visit_log_all_als_model_m";
        String zookeeper_quorum = "node2,node3,node4";
        String zookeeper_clientPort = "2181";
        String table = "law_visit_log_als_recommends";
        String cf = "recommends";
        String url_qualifier = "fullurl";
        String rating_qualifier = "rating";
        String[] args = new String[]{
                inputTableDB,
                inputTablePrefix,
                zookeeper_quorum,
                zookeeper_clientPort,
                table,
                cf,
                url_qualifier,
                rating_qualifier,
                appName
        };
        SparkYarnJob.runAndMonitor(appName,mainClass,args);
    }
}
