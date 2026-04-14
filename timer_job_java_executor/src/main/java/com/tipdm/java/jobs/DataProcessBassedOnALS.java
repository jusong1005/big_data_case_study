package com.tipdm.java.jobs;
import com.tipdm.engine.SparkYarnJob;
import com.tipdm.java.JavaJobInterface;
import com.xxl.job.core.biz.model.ReturnT;
import java.util.Map;
public class DataProcessBassedOnALS implements JavaJobInterface{
    @Override
    public ReturnT<String> execute(String className, Map<String, String> args) throws Exception {
        String mainClass = "com.tipdm.chapter_2_6_2_als_data_preprocess.DataProcessBasedOnAls";
        String appName = "als data preprocess";
        String output_model_data = args.get("--ismodeldata");//String.valueOf(output_model_data_boolean);
        String inputTable = args.get("--inputTable");//"law.law_visit_log_all_cleaned";
        String train_validate_percent = args.get("--train_validate_percent");//"0.8,0.2";
        String modelTrainTable = args.get("--modelTrainTable");//"law.law_visit_log_all_cleaned_als_train";
        String validateTrainTable = args.get("--validateTrainTable");//""law.law_visit_log_all_cleaned_als_validate_train"
        String validateTable = args.get("--validateTable");//"law.law_visit_log_all_cleaned_als_validate";
        String urlColName = args.get("--urlColName");//"fullurl";
        String timeColName = args.get("--timeColName");//"timestamp_format";
        String userMetaTable = args.get("--userMetaTable");//"law.law_visit_log_all_userMeta";
        String urlMetaTable = args.get("--urlMetaTable");//"law.law_visit_log_all_urlMeta";
        String userColName = args.get("--userColName");//"userid";
        String useridColName = args.get("--useridName");//"uid";
        String urlidColName = args.get("--urlidName");//"pid";
        String[] myArgs = new String[]{
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
        SparkYarnJob.runAndMonitor(appName,mainClass,myArgs);
        return ReturnT.SUCCESS;
    }
}
