package com.tipdm.java.label;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;

/**
 * Created by ch on 2018/9/19
 *
 * 地区用户统计的调用
 */
public class AreaStatistics {
    private final static String className = "com.tipdm.scala.statistics.AreaStatistics";
    private final static String applicationName = "User Area Count";
    
    public static SubmitResult run(String inputTable, String outputTable) {
        String[] arguments = new String[2];
        arguments[0] = inputTable;
        arguments[1] = outputTable;
        Args innerArgs = Args.getArgs(applicationName,className,arguments);
        SubmitResult submitResult = SparkYarnJob.run(innerArgs);

//        return SparkEngine.submit(applicationName, className, arguments);
        return submitResult;
    }

    public static void main(String[] args) throws Exception {
        SubmitResult applicationId = AreaStatistics.run("mediamatch_usermsg_process", "user_area_count");
        SparkYarnJob.monitor(applicationId);
    }
}
