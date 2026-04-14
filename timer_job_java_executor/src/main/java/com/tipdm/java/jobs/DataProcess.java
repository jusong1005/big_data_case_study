package com.tipdm.java.jobs;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.java.JavaJobInterface;
import com.xxl.job.core.biz.model.ReturnT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * //@Author:qwm
 * //@Date: 2018/11/13 9:44
 */
public class DataProcess implements JavaJobInterface {
    private static Logger log = LoggerFactory.getLogger(DataProcess.class);
    private static String sparkClassName = "com.tipdm.scala.processing.DataProcess";
    private static String applicationName = "DataProcess";

    public static void main(String[] args) throws Exception {
        String className = "com.tipdm.java.jobs.DataProcess";
        DataProcess dataProcess = new DataProcess();
        dataProcess.execute(className, null);
    }


    @Override
    public ReturnT<String> execute(String jobClassName, Map<String, String> args) throws Exception {
        String[] arguments = new String[10];
        arguments[0] = "media_index_3m";
        arguments[1] = "media_index_3m_process";
        arguments[2] = "mediamatch_userevent";
        arguments[3] = "mediamatch_userevent_process";
        arguments[4] = "mediamatch_usermsg";
        arguments[5] = "mediamatch_usermsg_process";
        arguments[6] = "mmconsume_billevents";
        arguments[7] = "mmconsume_billevent_process";
        arguments[8] = "order_index_v3";
        arguments[9] = "order_index_process";
//        String applicationId = SparkEngine.submit(applicationName, sparkClassName, arguments);
//        SparkEngine.monitor(applicationId);
        SparkYarnJob.runAndMonitor(applicationName,sparkClassName,arguments);
        log.info("任务运行成功");
        return ReturnT.SUCCESS;
    }

}
