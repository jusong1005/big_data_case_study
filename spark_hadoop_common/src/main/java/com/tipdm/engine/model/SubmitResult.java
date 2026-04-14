package com.tipdm.engine.model;

import com.tipdm.engine.engine.type.EngineType;

/**
 * 任务提交结果
 * @Author: fansy
 * @Time: 2018/12/6 10:45
 * @Email: fansy1990@foxmail.com
 */
public class SubmitResult {
    private String jobId;
    private EngineType engineType;

    private SubmitResult(String jobId, EngineType engineType){
        this.jobId = jobId;
        this.engineType = engineType;
    }

    public static SubmitResult getSubmitResult(String jobId, EngineType engineType){
        return new SubmitResult(jobId,engineType);
    }

    public String getJobId() {
        return jobId;
    }

    public EngineType getEngineType() {
        return engineType;
    }

}
