package com.tipdm.java.jobs;

import com.tipdm.java.JavaJobInterface;
import com.tipdm.utils.SparkUtils;
import com.xxl.job.core.biz.model.ReturnT;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Map;

public class EnvTest implements JavaJobInterface{
    String modelPath="/user/root/ringtest";

    public static void main(String[] args) throws Exception {
        EnvTest et = new EnvTest();
        String className="";
        et.execute(className,null);
    }

    @Override
    public ReturnT<String> execute(String className, Map<String, String> args) throws Exception {
        //String modelPath=args.get("--input");
        //SparkUtils.getFs().create(new Path(modelPath));
        FileSystem fs = SparkUtils.getFs();
        System.out.println(fs.getConf().get("fs.default.name"));
        SparkUtils.getFs().create(new Path(modelPath));
        return ReturnT.SUCCESS;
    }
}
