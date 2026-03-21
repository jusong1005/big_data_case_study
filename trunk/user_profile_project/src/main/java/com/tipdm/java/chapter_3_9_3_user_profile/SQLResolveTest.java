package com.tipdm.java.chapter_3_9_3_user_profile;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;

/**
 * //@Author:qwm
 * //@Date: 2019/2/14 10:39
 */
public class SQLResolveTest {
    private static String  className="com.tipdm.scala.chapter_3_9_3_user_profile.SQLEngine";
    private static  String applicationName = "SQLEngine";
    public static void main(String[] args) throws Exception {
        String[] arguments = new String[8];
        arguments[0] = "SQLEngine";
        arguments[1] = "select phone_no,run_time,sm_name,run_name from mediamatch_usermsg_process limit 10";
        arguments[2] = "sqlEngine_test";
        arguments[3] = "rdbms";
        arguments[4] = "overwrite";
        arguments[5] = "jdbc:mysql://192.168.111.75:3306/user_profile";
        arguments[6] = "root";
        arguments[7] = "root";
        Args innerArgs = Args.getArgs(applicationName,className,arguments);
        SubmitResult submitResult = SparkYarnJob.run(innerArgs);
        SparkYarnJob.monitor(submitResult);
        System.out.println("任务运行成功");
    }
}
