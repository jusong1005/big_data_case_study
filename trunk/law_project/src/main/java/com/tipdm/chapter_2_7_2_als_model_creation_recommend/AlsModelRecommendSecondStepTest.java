package com.tipdm.chapter_2_7_2_als_model_creation_recommend;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.utils.SparkUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 根据参数，构建ALS模型及进行推荐 测试
 *
 * @Author: fansy
 * @Time: 2019/2/18 14:52
 * @Email: fansy1990@foxmail.com
 */
public class AlsModelRecommendSecondStepTest {
    public static final Logger log = LoggerFactory.getLogger(AlsModelRecommendSecondStep.class);
    public static final String REC_TABLE_PREFIX = "law.law_visit_log_all_als_model_";
    public static String mainClass = "com.tipdm.chapter_2_7_2_als_model_creation_recommend.AlsModelRecommendSecondStep";
    public static String appName = "als model recommend ";

    public static void main(String[] args) throws IOException {
        test();
    }

    public static void test() throws IOException {
        int max_models = 1; // cluster with enough resources set this value to Integer.MAX_VALUE
        String models_path = "/tmp/spark/als_split_model";
        String uri = SparkUtils.getConf().get("fs.default.name");
        FileStatus[] fileStatuses = SparkUtils.getFs().listStatus(new Path(models_path));
        Path path = null;
        String modelPath, recTable;
        int i = 0;
        for (FileStatus fileStatus : fileStatuses) {
            path = fileStatus.getPath();
            modelPath = path.toString().replace(uri, "");
            recTable = REC_TABLE_PREFIX + path.getName();
            log.info("model ID:{}, modelPath:{}, recTable:{}", path.getName(), modelPath, recTable);
            run(modelPath, recTable);
            if (i >= max_models) {// 用于测试
                break;
            }
        }
        log.info("Job ends With {} models", i);

    }

    public static void run(String modelPath, String recPath) {
        String modelTable = modelPath;
        String useridCol = "uid";
        String urlidCol = "pid";
        String ratingCol = "rating";
        String userMetaTable = "law.law_visit_log_all_userMeta";
        String userCol = "userid";
        String urlMetaTable = "law.law_visit_log_all_urlMeta";
        String urlCol = "fullurl";
        String recNum = "10";
        String recTable = recPath;
        String partitionSize = "110";
        String[] args = new String[]{
                modelTable,
                useridCol,
                urlidCol,
                ratingCol,
                userMetaTable,
                userCol,
                urlMetaTable,
                urlCol,
                recNum,
                recTable,
                partitionSize,
                appName
        };
        SparkYarnJob.runAndMonitor(appName, mainClass, args);
    }
}
