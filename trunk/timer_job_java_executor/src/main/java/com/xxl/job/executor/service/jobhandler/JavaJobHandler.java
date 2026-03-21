package com.xxl.job.executor.service.jobhandler;

import com.tipdm.java.JavaJobInterface;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.JobHandler;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.xxl.job.core.log.XxlJobLogger.log;


/**
 * 任务Handler示例（Bean模式）
 *
 * 参数： <class param_key param_value param_key param_value>
 *
 * 开发步骤：
 * 1、继承"IJobHandler"：“com.xxl.job.core.handler.IJobHandler”；
 * 2、注册到Spring容器：添加“@Component”注解，被Spring容器扫描为Bean实例；
 * 3、注册到执行器工厂：添加“@JobHandler(value="自定义jobhandler名称")”注解，注解value值对应的是调度中心新建任务的JobHandler属性的值。
 * 4、执行日志：需要通过 "XxlJobLogger.log" 打印执行日志；
 *
 * @author xuxueli 2015-12-19 19:43:36
 */
@JobHandler(value="javaJobHandler")
@Component
public class JavaJobHandler extends IJobHandler {
    private static final String CLASSINDEX = "com.tipdm.java.jobs.";
	@Override
	public ReturnT<String> execute(String  param) throws Exception {
	    String[] params = param.split(" ");
	    if(params.length < 1){
	        log("请设置ClassName参数!");
	        return FAIL;
        }
        String className = params[0];
	    if(className.indexOf(CLASSINDEX) != 0){
	        log("ClassName 应该在 {0} 中", CLASSINDEX);
	        return FAIL;
        }
		log("ClassName:"+className);
        Class<?> classes = Class.forName(className);
        JavaJobInterface job = (JavaJobInterface) classes.newInstance();

        Map<String,String> args = handleArgs(Arrays.copyOfRange(params,1,params.length));

		return job.execute(className,args);
	}

    private Map<String, String> handleArgs(String[] args) {

	    Map<String,String> result  = new HashMap<>();
        for(int i =0 ;i < args.length ; i=i+2 ){
            result.put(args[i],args[i+1]);
        }
        return result;
    }

}
