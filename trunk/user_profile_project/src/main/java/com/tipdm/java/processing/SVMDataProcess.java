package com.tipdm.java.processing;
import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;
/**
 * Created by ch on 2019/1/24
 * SVM数据预处理调用
 */
public class SVMDataProcess {
    private static String className = "com.tipdm.scala.processing.SVMDataProcess";
    private static String applicationName = "SVMDataProcess";
    public static void main(String[] args) throws Exception {
        String[] arguments = new String[7];
        arguments[0] = "mmconsume_billevent_process";
        arguments[1] = "mediamatch_userevent_process";
        arguments[2] = "media_index_3m_process";
        arguments[3] = "mediamatch_usermsg_process";
        arguments[4] = "order_index_process";
        arguments[5] = "svm_train";
        arguments[6] = "svm_toBePredicted";
        Args innerArgs = Args.getArgs(applicationName,className,arguments);
        SubmitResult submitResult = SparkYarnJob.run(innerArgs);
        SparkYarnJob.monitor(submitResult);
        System.out.println("任务运行成功");
    }
}
