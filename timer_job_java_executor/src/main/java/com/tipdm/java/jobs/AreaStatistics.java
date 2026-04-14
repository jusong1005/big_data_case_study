package com.tipdm.java.jobs;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.java.JavaJobInterface;
import com.xxl.job.core.biz.model.ReturnT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * //@Author:qwm
 * //@Date: 2018/11/14 10:51
 */
public class AreaStatistics implements JavaJobInterface {
    private static Logger log = LoggerFactory.getLogger(AreaStatistics.class);
    private static String sparkClassName = "com.tipdm.scala.statistics.AreaStatistics";
    private static String applicationName = "AreaStatistics";

    public static void main(String[] args) throws Exception {
        String className = "com.tipdm.java.jobs.AreaStatistics";
        AreaStatistics areaStatistics = new AreaStatistics();
        areaStatistics.execute(className, null);
    }


    @Override
    public ReturnT<String> execute(String jobClassName, Map<String, String> args) throws Exception {
        String[] arguments = new String[2];
        arguments[0] = "mediamatch_usermsg_process";
        arguments[1] = "areastatics_out";
//        String applicationId = SparkEngine.submit(applicationName, sparkClassName, arguments);
//        SparkEngine.monitor(applicationId);
        SparkYarnJob.runAndMonitor(applicationName,sparkClassName,arguments);
        log.info("任务运行成功");
        return ReturnT.SUCCESS;
    }
}
