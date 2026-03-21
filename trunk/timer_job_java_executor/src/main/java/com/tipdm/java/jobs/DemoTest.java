package com.tipdm.java.jobs;

import com.tipdm.java.JavaJobInterface;
import com.xxl.job.core.biz.model.ReturnT;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;

/**
 * 示例Java executor任务
 * @Author: fansy
 * @Time: 2019/1/22 15:31
 * @Email: fansy1990@foxmail.com
 */
public class DemoTest implements JavaJobInterface {

    @Override
    public ReturnT<String> execute(String className, Map<String, String> args) throws Exception {

        String input = args.get("--input");
        String info = args.get("--info");
        File file =new File(input);
        // if file doesn't exists, then create it
        if(!file.exists()){
            file.createNewFile();
        }
        // true = append file
        FileWriter fileWriter = new FileWriter(file,true);
        BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
        bufferWriter.write(info);
        bufferWriter.close();
        return ReturnT.SUCCESS;
    }
}
