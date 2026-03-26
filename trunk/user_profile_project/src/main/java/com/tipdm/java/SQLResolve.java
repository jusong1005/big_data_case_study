package com.tipdm.java;


import com.tipdm.engine.SparkYarnJob;
import com.tipdm.engine.engine.type.EngineType;
import com.tipdm.engine.model.Args;
import com.tipdm.engine.model.SubmitResult;
import com.tipdm.java.label.LabelSQL;

import java.util.HashMap;
import java.util.Map;


/**
 * Created by ch on 2018/8/29
 *
 * 标签的调用
 */
public class SQLResolve {
    private static String className = "com.tipdm.scala.SQLEngine";
    private static String applicationName = "SQLResolve";

    public static void main(String[] args) throws Exception {
        Map<String, String> labelTable = new HashMap<>();
        labelTable.put("消费内容", "mmconsume_billevent_process");
        labelTable.put("电视消费水平", "mmconsume_billevent_process");
        labelTable.put("宽带消费水平", "mmconsume_billevent_process");
        labelTable.put("宽带产品带宽", "order_index_process");
        labelTable.put("用户是否挽留","svm_prediction");
        labelTable.put("销售品名称", "order_index_process");
        labelTable.put("业务品牌", "mediamatch_usermsg_process");
        labelTable.put("电视入网程度", "mediamatch_usermsg_process");
        labelTable.put("宽带入网程度", "mediamatch_usermsg_process");
        int flag = 0;
        String[] arguments = new String[8];
        String sql = null;
        for (Map.Entry<String, String> entry : labelTable.entrySet()) {
            sql = LabelSQL.getLabel(entry.getKey(), entry.getValue());
            System.out.println("SQL：" + sql);
            if (flag == 0) {
                arguments[4] = "overwrite";
            } else {
                arguments[4] = "append";
            }
            arguments[0] = "LabelName：" + entry.getKey();
            arguments[1] = sql;
            arguments[2] = "user_label";
            arguments[3] = "rdbms";
            arguments[5] = "jdbc:mysql://192.168.2.162:3306/zjsm?useUnicode=true&characterEncoding=UTF-8&useSSL=false&allowPublicKeyRetrieval=true";
            arguments[6] = "root";
            arguments[7] = "root";
//            String applicationId = SparkEngine.submit(applicationName, className, arguments);
//            SparkEngine.monitor(applicationId);

            Args innerArgs = Args.getArgs(applicationName,className,arguments, EngineType.SPARK);
            SubmitResult submitResult = SparkYarnJob.run(innerArgs);
            SparkYarnJob.monitor(submitResult);

            flag++;
            // 间隔10秒钟 发起下一个任务
            Thread.sleep(1000 * 10L);
        }
        System.out.println("任务运行成功");
    }

}
