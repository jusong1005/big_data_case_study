package com.tipdm.dataclean;

import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;

public class ALSRecommend {
    public static void main(String[] args) {
        String mainClass = "com.tipdm.model.ALSModelRecommend";
        String appName = "law_";
        String[] myArgs = new String[12];
        myArgs[0] = "/user/root/ALSModelPath";                //模型存储路径
        myArgs[1] = "10";                                         //推荐个数
        myArgs[2] = "law_init.data_101003_usermeta";          //用户元数据
        myArgs[3] = "law_init.data_101003_itemmeta";          //网页元数据
        myArgs[4] = "uid";                                         //训练集中用户自字段名
        myArgs[5] = "pid";                                         //训练集中网页字段名
        myArgs[6] = "id";                                          //用户元数据表用户编号列
        myArgs[7] = "id";                                          //网页元数据表网页编号列
        myArgs[8] = "user";                                        //用户元数据表用户列
        myArgs[9] = "item";                                        //网页元数据表网页列
        myArgs[10] = "/user/root/recommendALSPath";            //推荐结果存储
        myArgs[11] = "law_init.data_101003_encode_als";        //训练数据
//        String jobid = SparkEngine.submit(appName, mainClass, myArgs);
//        try {
//            SparkEngine.monitor(jobid);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        Args innerArgs = Args.getArgs(appName,mainClass,myArgs);
        SubmitResult submitResult = SparkYarnJob.run(innerArgs);
        SparkYarnJob.monitor(submitResult);
    }
}
