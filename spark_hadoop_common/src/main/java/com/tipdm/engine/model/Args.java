package com.tipdm.engine.model;


import com.tipdm.engine.engine.type.EngineType;

import java.util.ArrayList;
import java.util.List;

import static com.tipdm.utils.SparkUtils.getValue;

/**
 * 参数公共父类
 *
 * @Author: fansy
 * @Time: 2018/12/6 10:44
 * @Email: fansy1990@foxmail.com
 */
public class Args {
    private static EngineType DEFAULT_ENGINE_TYPE = null;

    static {
        DEFAULT_ENGINE_TYPE = EngineType.valueOf(getValue("job.engine").toUpperCase());
    }

    private String appName;// 任务名称
    private String[] args;
    private EngineType engineType = DEFAULT_ENGINE_TYPE;// 算法引擎
    private String mainClass;

    private Args() {
    }

    /**
     * 构造参数，默认引擎
     *
     * @param appName
     * @param mainClass
     * @param args
     * @return
     */
    public static Args getArgs(String appName, String mainClass, String[] args) {
        return getArgs(appName, mainClass, args, DEFAULT_ENGINE_TYPE);
    }

    /**
     * 构造参数,显示指定执行引擎
     *
     * @param appName
     * @param mainClass
     * @param args
     * @param engineType
     * @return
     */
    public static Args getArgs(String appName, String mainClass, String[] args, EngineType engineType) {
        Args innerArgs = new Args();
        innerArgs.setAppName(appName);
        innerArgs.setMainClass(mainClass);
        innerArgs.setArgs(args);
        innerArgs.setEngineType(engineType);
        return innerArgs;
    }

    /**
     * 返回 YARN引擎所需参数
     * @return
     */
    public String[] argsForYarn(){
        int defaultSize = 16;
        int defaultClassArgsLength = args.length * 2;

        List<String> argsList = new ArrayList<String>() {{
            add("--name");
            add(appName);
            add("--class");
            add(mainClass);
            add("--driver-memory");
            add(getValue("yarn.spark.driver.memory"));
            add("--num-executors");
            add(getValue("yarn.spark.num.executors"));
            add("--executor-memory");
            add(getValue("yarn.spark.executor.memory"));
            add("--executor-cores");
            add(getValue("yarn.spark.executor.cores"));
            add("--jar");
            add(getValue("yarn.spark.algorithm.jar"));
            add("--files");
            add(getValue("yarn.spark.files"));
        }};


        int j = defaultSize;
        for (int i = 0; i < defaultClassArgsLength / 2; i++) {
            argsList.add("--arg");
            argsList.add(args[i]);
        }
        return argsList.toArray(new String[0]);
    }


    public String[] getArgs() {
        return args;
    }

    public void setArgs(String[] args) {
        this.args = args;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public EngineType getEngineType() {
        return engineType;
    }

    public void setEngineType(EngineType engineType) {
        this.engineType = engineType;
    }

    public String getMainClass() {
        return mainClass;
    }

    public void setMainClass(String mainClass) {
        this.mainClass = mainClass;
    }
}
