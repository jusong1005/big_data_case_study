package com.tipdm.java.processing;
import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;
/**
 * //@Author:qwm
 * //@Date: 2018/09/17 15:32
 *
 * 数据预处理的调用
 */

public class DataProcess {
    private static String className = "com.tipdm.scala.processing.DataProcess";
    private static String applicationName = "DataProcess";
    public static void main(String[] args) throws Exception {
        String[] arguments = new String[10];
        arguments[0] = "user_profile.media_index_3m";
        arguments[1] = "user_profile.media_index_3m_process";
        arguments[2] = "user_profile.mediamatch_userevent";
        arguments[3] = "user_profile.mediamatch_userevent_process";
        arguments[4] = "user_profile.mediamatch_usermsg";
        arguments[5] = "user_profile.mediamatch_usermsg_process";
        arguments[6] = "user_profile.mmconsume_billevents";
        arguments[7] = "user_profile.mmconsume_billevent_process";
        arguments[8] = "user_profile.order_index_v3";
        arguments[9] = "user_profile.order_index_process";
        Args innerArgs = Args.getArgs(applicationName,className,arguments);
        SubmitResult submitResult = SparkYarnJob.run(innerArgs);
        SparkYarnJob.monitor(submitResult);
        System.out.println("任务运行成功");
    }
}
