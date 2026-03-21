package com.tipdm.chapter_2_7_2_als_model_creation_recommend;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.utils.SparkUtils;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * 根据参数，构建ALS模型及进行推荐 测试
 * @Author: fansy
 * @Time: 2019/2/18 14:52
 * @Email: fansy1990@foxmail.com
 */
public class AlsModelCreationFirstStepTest {
    public static void main(String[] args) throws IOException {
        test();
    }
    public static void test() throws IOException {
        String mainClass = "com.tipdm.chapter_2_7_2_als_model_creation_recommend.AlsModelCreationFirstStep";
        String appName = "als model creation first step ";
        String inputTable = "law.law_visit_log_all_cleaned_als_train";
        String useridCol = "uid";
        String urlidCol = "pid";
        String ratingCol = "rating";
        String url = "jdbc:mysql://node1:3306/law?characterEncoding=UTF-8";
        String table = "als_model_args";
        String user = "root";
        String password = "root";
        String modelTable = "/tmp/spark/als_split_model";
        String[] args = new String[]{
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
        SparkYarnJob.runAndMonitor(appName,mainClass,args);
    }
}
