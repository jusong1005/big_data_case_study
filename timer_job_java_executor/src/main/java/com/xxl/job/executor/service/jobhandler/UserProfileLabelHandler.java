package com.xxl.job.executor.service.jobhandler;

import com.tipdm.java.JavaJobInterface;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHandler;
import org.springframework.stereotype.Component;

import static com.xxl.job.core.log.XxlJobLogger.log;

@JobHandler(value = "userProfileLabelHandler")
@Component
public class UserProfileLabelHandler extends IJobHandler {

    @Override
    public ReturnT<String> execute(String param) throws Exception {
        log("开始执行用户画像标签T+1定时计算...");
        String className = "com.tipdm.java.jobs.user_profile.label.SQLResolve";
        Class<?> clazz = Class.forName(className);
        JavaJobInterface job = (JavaJobInterface) clazz.newInstance();
        return job.execute(className, null);
    }
}
