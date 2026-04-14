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
 * //@Date: 2019/2/18 15:31
 */
public class Billevents2Hive implements JavaJobInterface {
    private static Logger log = LoggerFactory.getLogger(Billevents2Hive.class);
    private static String sparkClassName = "com.tipdm.scala.chatper_3_5_1_datasource.Elasticsearch2Hive";
    private static String applicationName = "Billevents2Hive";

    public static void main(String[] args) throws Exception {
        String className = "com.tipdm.java.jobs.user_profile.datasource.Billevents2Hive";
        Billevents2Hive billevents2Hive = new Billevents2Hive();
        billevents2Hive.execute(className, null);
    }


    @Override
    public ReturnT<String> execute(String jobClassName, Map<String, String> args) throws Exception {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String[] arguments =new String [9];
        arguments[0]="mmconsume_billevents";
        arguments[1]="terminal_no,phone_no,fee_code,year_month,owner_name,owner_code,sm_name,should_pay,favour_fee";
        arguments[2]="year_month";
        arguments[3]="yyyy-MM-dd HH:mm:ss";
        arguments[4]="1";
        arguments[5]="M";
        arguments[6]="mmconsume_billevents_update/doc";
        arguments[7]=df.format(new Date());
        arguments[8]="user_profile";
        SparkYarnJob.runAndMonitor(applicationName,sparkClassName,arguments);
        log.info("任务运行成功");
        return ReturnT.SUCCESS;
    }

}

