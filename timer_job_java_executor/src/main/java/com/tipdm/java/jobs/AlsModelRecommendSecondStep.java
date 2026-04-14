package com.tipdm.java.jobs;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.java.JavaJobInterface;
import com.tipdm.utils.SparkUtils;
import com.xxl.job.core.biz.model.ReturnT;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class AlsModelRecommendSecondStep implements JavaJobInterface {
    public static final Logger log = LoggerFactory.getLogger(AlsModelRecommendSecondStep.class);
    public static final String REC_TABLE_PREFIX = "law.law_visit_log_als_model_";
    public static String mainClass = "com.tipdm.chapter_2_7_2_als_model_creation_recommend.AlsModelRecommendSecondStep";
    public static String appName = "als model recommend ";


    @Override
    public ReturnT<String> execute(String className, Map<String, String> args) throws Exception {
        int max_models = Integer.parseInt(args.get("--max_model"));//2;
        String models_path = args.get("--models_path");//"/tmp/spark/als_split_model";

        String useridCol = args.get("--useridCol");//"uid";
        String urlidCol = args.get("--urlidCol");//"pid";
        String ratingCol = args.get("--ratingCol");//"rating";
        String userMetaTable = args.get("--userMetaTable");//"law.law_visit_log_all_userMeta";
        String userCol = args.get("--userCol");//"userid";
        String urlMetaTable = args.get("--urlMetaTable");//"law.law_visit_log_all_urlMeta";
        String urlCol = args.get("--urlCol");//"fullurl";
        String recNum = args.get("--recNum");//"10";
        String partitionSize = args.get("--partitionSize");//"110";
        String uri = SparkUtils.getConf().get("fs.default.name");
        FileStatus[] fileStatuses = SparkUtils.getFs().listStatus(new Path(models_path));
        Path path = null;
        String modelPath, recTable;
        int i =0;
        for (FileStatus fileStatus : fileStatuses) {
            path = fileStatus.getPath();
            modelPath = path.toString().replace(uri, "");
            recTable = REC_TABLE_PREFIX + path.getName();
            log.info("model ID:{}, modelPath:{}, recTable:{}", path.getName(), modelPath, recTable);
            String modelTable = modelPath;
            String[] myArgs = new String[]{
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
            SparkYarnJob.runAndMonitor(appName, mainClass, myArgs);;
            if(i >= max_models){// 用于测试
                break;
            }
            i++;
        }
        log.info("Job ends With {} models", i);
        return ReturnT.SUCCESS;
    }
}
