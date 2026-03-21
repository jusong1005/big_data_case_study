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
 * //@Date: 2019/2/18 15:36
 */
public class Userevent2Hive implements JavaJobInterface {
    private static Logger log = LoggerFactory.getLogger(Userevent2Hive.class);
    private static String sparkClassName = "com.tipdm.scala.chatper_3_5_1_datasource.Elasticsearch2Hive";
    private static String applicationName = "Userevent2Hive";
    public static void main(String[] args) throws Exception {
        String className = "com.tipdm.java.jobs.user_profile.datasource.Userevent2Hive";
        Userevent2Hive userevent2Hive = new Userevent2Hive();
        userevent2Hive.execute(className, null);
    }
    @Override
    public ReturnT<String> execute(String jobClassName, Map<String, String> args) throws Exception {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String[] arguments =new String [9];
        arguments[0]="mediamatch_userevent";
        arguments[1]="phone_no,run_name,run_time,owner_name,owner_code,open_time";
        arguments[2]="run_time";
        arguments[3]="yyyy-MM-dd HH:mm:ss";
        arguments[4]="10";
        arguments[5]="D";
        arguments[6]="mediamatch_userevent/doc";
        arguments[7]=df.format(new Date());
        arguments[8]="user_profile";
        SparkYarnJob.runAndMonitor(applicationName,sparkClassName,arguments);
        log.info("任务运行成功");
        return ReturnT.SUCCESS;
    }
}
