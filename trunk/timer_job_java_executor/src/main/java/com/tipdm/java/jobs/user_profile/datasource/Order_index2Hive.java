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
 * //@Date: 2019/2/18 15:39
 */
public class Order_index2Hive implements JavaJobInterface {
    private static Logger log = LoggerFactory.getLogger(Order_index2Hive.class);
    private static String sparkClassName = "com.tipdm.scala.chatper_3_5_1_datasource.Elasticsearch2Hive";
    private static String applicationName = "Order_index2Hive";

    public static void main(String[] args) throws Exception {
        String className = "com.tipdm.java.jobs.user_profile.datasource.Order_index2Hive";
        Order_index2Hive order_index2Hive = new Order_index2Hive();
        order_index2Hive.execute(className, null);
    }


    @Override
    public ReturnT<String> execute(String jobClassName, Map<String, String> args) throws Exception {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String[] arguments =new String [9];
        arguments[0]="order_index_v3";
        arguments[1]="phone_no,owner_name,optdate,prodname,sm_name,offerid,offername,business_name,owner_code,prodprcid,prodprcname,effdate,expdate,orderdate,cost,mode_time,prodstatus,run_name,orderno,offertype";
        arguments[2]="optdate";
        arguments[3]="yyyy-MM-dd HH:mm:ss";
        arguments[4]="3";
        arguments[5]="M";
        arguments[6]="order_test4/doc";
        arguments[7]=df.format(new Date());
        arguments[8]="user_profile";
        SparkYarnJob.runAndMonitor(applicationName,sparkClassName,arguments);
        log.info("任务运行成功");
        return ReturnT.SUCCESS;
    }
}
