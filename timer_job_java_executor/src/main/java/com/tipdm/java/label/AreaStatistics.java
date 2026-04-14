package com.tipdm.java.label;

import com.tipdm.engine.SparkYarnJob;

/**
 * Created by ch on 2018/9/19
 */
public class AreaStatistics {
    private final static String className = "com.tipdm.scala.statistics.AreaStatistics";
    private final static String applicationName = "User Area Count";
    
    /*public static String run(String inputTable, String outputTable) {
        String[] arguments = new String[2];
        arguments[0] = inputTable;
        arguments[1] = outputTable;
        return SparkEngine.submit(applicationName, className, arguments);
    }*/

    public static void main(String[] args) throws Exception {
//        String applicationId = AreaStatistics.run("mediamatch_usermsg_process", "user_area_count");
//        SparkEngine.monitor(applicationId);

        SparkYarnJob.runAndMonitor(applicationName,className,new String[]{"mediamatch_usermsg_process", "user_area_count"});
    }
}
