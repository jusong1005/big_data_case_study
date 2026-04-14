package com.tipdm.java.jobs.user_profile.datasource;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.java.JavaJobInterface;
import com.xxl.job.core.biz.model.ReturnT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * //@Author:qwm
 * //@Date: 2019/2/18 15:49
 */
public class MediaIndex2Hive implements JavaJobInterface {
    private static Logger log = LoggerFactory.getLogger(MediaIndex2Hive.class);
    private static String sparkClassName = "com.tipdm.scala.chatper_3_5_1_datasource.ElasticsearchMulti2Hive";
    private static String applicationName = "MediaIndex2Hive";

    public static void main(String[] args) throws Exception {
        String className = "com.tipdm.java.jobs.user_profile.datasource.MediaIndex2Hive";
        MediaIndex2Hive mediaIndex2Hive = new MediaIndex2Hive();
        mediaIndex2Hive.execute(className, null);
    }


    @Override
    public ReturnT<String> execute(String jobClassName, Map<String, String> args) throws Exception {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String[] arguments =new String [11];
        arguments[0]="media_index_3m";
        arguments[1]="terminal_no,phone_no,duration,station_name,origin_time,end_time,owner_code,owner_name,vod_cat_tags,resolution,audio_lang,region,res_name,res_type,vod_title,category_name,program_title,sm_name,first_show_time";
        arguments[2]="origin_time";
        arguments[3]="yyyy-MM-dd HH:mm:ss";
        arguments[4]="10";
        arguments[5]="D";
        arguments[6]="yyyyww";
        arguments[7]="media_index";
        arguments[8]="media";
        arguments[9]=df.format(new Date());
        arguments[10]="user_profile";
        SparkYarnJob.runAndMonitor(applicationName,sparkClassName,arguments);
        log.info("任务运行成功");
        return ReturnT.SUCCESS;
    }
}
