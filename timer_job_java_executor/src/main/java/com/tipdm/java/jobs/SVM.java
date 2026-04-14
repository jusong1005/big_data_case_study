package com.tipdm.java.jobs;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.java.JavaJobInterface;
import com.xxl.job.core.biz.model.ReturnT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * //@Author:qwm
 * //@Date: 2018/11/13 9:50
 */
public class SVM implements JavaJobInterface {
    private static Logger log = LoggerFactory.getLogger(SVM.class);
    private static String sparkClassName = "com.tipdm.scala.svm.SVM";
    private static String applicationName = "SVM";

    public static void main(String[] args) throws Exception {
        String className = "com.tipdm.java.jobs.SVM";
        SVM svm = new SVM();
        svm.execute(className, null);
    }

    @Override
    public ReturnT<String> execute(String jobClassName, Map<String, String> args) throws Exception {
        String[] arguments = new String[7];
        arguments[0] = "mmconsume_billevent_process";
        arguments[1] = "mediamatch_userevent_process";
        arguments[2] = "media_index_3m_process";
        arguments[3] = "mediamatch_usermsg_process";
        arguments[4] = "order_index_process";
        arguments[5] = "svm_activate";
        arguments[6] = "svm_prediction";
//        String applicationId = SparkEngine.submit(applicationName, sparkClassName, arguments);
//        SparkEngine.monitor(applicationId);
        SparkYarnJob.runAndMonitor(applicationName,sparkClassName,arguments);
        log.info("任务运行成功");
        return ReturnT.SUCCESS;
    }

}
