package com.tipdm.engine;

import com.tipdm.engine.engine.type.EngineType;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.rest.SparkEngine;
import org.apache.spark.deploy.rest.SubmissionStatusResponse;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

import static com.tipdm.utils.SparkUtils.*;
/**
 * 算法调用引擎，支持声明式或默认引擎选择；
 * @Author: fansy
 * @Time: 2018/12/6 10:43
 * @Email: fansy1990@foxmail.com
 */
public class SparkYarnJob {
    private static final Logger logger = LoggerFactory.getLogger(SparkYarnJob.class);
    /**
     * 提交任务
     * @param args
     * @return
     */
    public static SubmitResult run(Args args){
        switch (args.getEngineType()){
            case YARN:
                System.setProperty("SPARK_YARN_MODE", "true");
                SparkConf sparkConf = getSparkConf(EngineType.YARN);
                ClientArguments cArgs = new ClientArguments(args.argsForYarn(), sparkConf);
                Client client = new Client(cArgs, getConf(), sparkConf);
                ApplicationId appId = client.submitApplication();
                return SubmitResult.getSubmitResult(appId.toString(),args.getEngineType());

            case SPARK:
                String jobId = SparkEngine.run(args.getAppName(), args.getMainClass(), args.getArgs());
                return SubmitResult.getSubmitResult(jobId, args.getEngineType());

            default:
                logger.error("Algorithm Engine Failed!");
        }
        return SubmitResult.getSubmitResult(null,null);
    }

    /**
     * 提交任务并监控
     * @param appName
     * @param mainClass
     * @param args
     */
    public static void runAndMonitor(String appName,String mainClass,String[] args){
        Args innerArgs = Args.getArgs(appName,mainClass,args);
        SubmitResult submitResult = SparkYarnJob.run(innerArgs);
        SparkYarnJob.monitor(submitResult);
    }

    private static long getRandomInterval(){
        int interval = Integer.parseInt(getValue("job.check.interval")) * 1000;
        return random.nextInt(interval);
    }
    private static Random random = new Random();
    /**
     * 监控任务
     * @param jobInfo
     */
    public static void monitor(SubmitResult jobInfo){
        boolean finished =false;
        switch (jobInfo.getEngineType()){
            case YARN:
                while (!finished) {
                    try {
                        logger.info("Checking Job {} , running...", jobInfo.getJobId() );
                        Thread.sleep(getRandomInterval());
                        FinalApplicationStatus applicationStatus = getFinalStatus(jobInfo.getJobId());
                        switch (applicationStatus) {
                            case SUCCEEDED:
                                logger.info("=== {} 成功运行并完成!",jobInfo.getJobId());
                                finished = true;
                                break;
                            case FAILED:
                                logger.warn("=== {} 运行异常!",jobInfo.getJobId());
                                cleanupStagingDir(jobInfo.getJobId());
                                finished = true;
                                break;
                            case KILLED:
                                logger.warn("=== {} 任务被杀死!", jobInfo.getJobId());
                                cleanupStagingDir(jobInfo.getJobId());
                                finished = true;
                                break;
                            case UNDEFINED: // 继续检查
                                break;
                            default:
                                logger.error("=== {} 任务状态获取异常!", jobInfo.getJobId());
                                cleanupStagingDir(jobInfo.getJobId());
                                finished = true;
                                break;
                        }
                    } catch (InterruptedException | YarnException | IOException e) {
                        e.printStackTrace();
                    }
                }
            case SPARK:
                SubmissionStatusResponse response = null;
                while(!finished) {
                    response = (SubmissionStatusResponse) getRestSubmissionClient().requestSubmissionStatus(jobInfo.getJobId(), true);
                    logger.info("DriverState :{}",response.driverState());
                    if("FINISHED" .equals(response.driverState()) ){
                        finished = true;
                    }
                    if( "ERROR".equals(response.driverState())){
                        finished = true;
//                        throw new Exception("任务异常!");
                    }
                    if( "FAILED".equals(response.driverState())){
                        finished = true ;
//                        throw new Exception("任务失败!");
                    }
                    try {
                        Thread.sleep(getRandomInterval());
                    }catch (InterruptedException e){
                        e.printStackTrace();
                    }
                }
                logger.info("Spark Engine Monitor done!");
                break;

                default:

        }
    }
}
