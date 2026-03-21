package com.tipdm.java.chapter_3_8_6_svm;
import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;
/**
 * //@Author:qwm
 * //@Date: 2018/09/17 09:33
 *
 * 提交SVM算法到Spark集群
 */
public class SVM {
    private static String  className="com.tipdm.scala.chapter_3_8_6_svm.SVM";
    private static  String applicationName = "SVM";
    public static void main(String[] args) throws Exception {
        String[] arguments =new String [12];
        arguments[0]="mmconsume_billevent_process";
        arguments[1]="mediamatch_userevent_process";
        arguments[2]="media_index_3m_process";
        arguments[3]="mediamatch_usermsg_process";
        arguments[4]="order_index_process";
        arguments[5]="svm_activate";
        arguments[6]="svm_prediction";
        arguments[7]="10";
        arguments[8]="1.0";
        arguments[9]="0.01";
        arguments[10]="1.0";
        arguments[11]="user_profile";
//        String applicationId = SparkEngine.submit(applicationName,className,arguments);
//        SparkEngine.monitor(applicationId);
        Args innerArgs = Args.getArgs(applicationName,className,arguments);
        SubmitResult submitResult = SparkYarnJob.run(innerArgs);
        SparkYarnJob.monitor(submitResult);
        System.out.println("任务运行成功");
    }
}
