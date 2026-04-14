package com.tipdm.java.chatper_3_5_1_datasource;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;

/**
 * //@Author:qwm
 * //@Date: 2018/09/17 10:22
 */

public class ElasticsearchMulti2Hive {
    private static String className = "com.tipdm.scala.chatper_3_5_1_datasource.ElasticsearchMulti2Hive";
    private static String applicationName = "ElasticsearchMulti2Hive";

    public static void main(String[] args) throws Exception {
        String[] arguments = new String[11];
        arguments[0] = "media_index_3m_test";
        arguments[1] = "terminal_no,phone_no,duration,station_name,origin_time,end_time,owner_code,owner_name,vod_cat_tags,resolution,audio_lang,region,res_name,res_type,vod_title,category_name,program_title,sm_name,first_show_time";
        arguments[2] = "origin_time";
        arguments[3] = "yyyy-MM-dd HH:mm:ss";
        arguments[4] = "1";
        arguments[5] = "D";
        arguments[6] = "yyyyww";
        arguments[7] = "media_index";
        arguments[8] = "media";
        arguments[9] = "2018-08-01 00:00:00";
        arguments[10]="user_profile";
        Args innerArgs = Args.getArgs(applicationName,className,arguments);
        SubmitResult submitResult = SparkYarnJob.run(innerArgs);
        SparkYarnJob.monitor(submitResult);

        System.out.println("任务运行成功");
    }
}
