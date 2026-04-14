package com.tipdm.java.jobs.user_profile.label;
import com.tipdm.engine.SparkYarnJob;
import com.tipdm.java.JavaJobInterface;
import com.xxl.job.core.biz.model.ReturnT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;
/**
 * Created by ch on 2018/8/29
 */
public class SQLResolve implements JavaJobInterface {
    private static Logger log = LoggerFactory.getLogger(SQLResolve.class);
    private static String sparkClassName = "com.tipdm.scala.chapter_3_9_3_user_profile.SQLEngine";
    private static String applicationName = "SQLResolve";

    public static void main(String[] args) throws Exception {
        String className = "com.tipdm.java.jobs.user_profile.label.SQLResolve";
        SQLResolve sqlResolve = new SQLResolve();
        sqlResolve.execute(className, null);
    }
    @Override
    public ReturnT<String> execute(String jobClassName, Map<String, String> args) throws Exception {
        Map<String, String> labelTable = new HashMap<>();
        labelTable.put("消费内容", "user_profile.mmconsume_billevent_process");
        labelTable.put("电视消费水平", "user_profile.mmconsume_billevent_process");
        labelTable.put("宽带消费水平", "user_profile.mmconsume_billevent_process");
        labelTable.put("宽带产品带宽", "user_profile.order_index_process");
        labelTable.put("用户是否挽留","user_profile.svm_prediction");
        labelTable.put("销售品名称", "user_profile.order_index_process");
        labelTable.put("业务品牌", "user_profile.mediamatch_usermsg_process");
        labelTable.put("电视入网程度", "user_profile.mediamatch_usermsg_process");
        labelTable.put("宽带入网程度", "user_profile.mediamatch_usermsg_process");
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
//            String applicationId = SparkEngine.submit(applicationName, sparkClassName, arguments);
//            SparkEngine.monitor(applicationId);
            SparkYarnJob.runAndMonitor(applicationName,sparkClassName,arguments);
            flag++;
            Thread.sleep(1000 * 60L);
        }
//        String[] arguments={applicationName,sql,"label1","hive","overwrite"};
        log.info("任务运行成功");
        return ReturnT.SUCCESS;
    }
}
