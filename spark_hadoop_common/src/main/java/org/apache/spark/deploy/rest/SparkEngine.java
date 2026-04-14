package org.apache.spark.deploy.rest;
import com.tipdm.engine.engine.type.EngineType;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.tipdm.utils.SparkUtils.*;

//import scala.Predef;
//import scala.Tuple2;
//import scala.collection.JavaConverters;

/**
 * Spark 引擎：
 * 1）调用Spark算法,提交任务到Spark StandAlone集群，并返回id；
 * 2）根据id监控Spark 任务；
 * Created by fanzhe on 2018/1/2.
 */
public class SparkEngine {

    private static final Logger log = LoggerFactory.getLogger(SparkEngine.class);

    /**
     * 提交任务
     * @param appName
     * @param mainClass
     * @param args
     * @return
     */
    public static String run(String appName,String mainClass,String[] args){
        log.info("run args:\n"+ Arrays.toString(args));
        SparkConf sparkConf = getSparkConf(EngineType.SPARK);
        sparkConf.setAppName(appName +" "+ System.currentTimeMillis());
        Map<String,String> env = filterSystemEnvironment(System.getenv());
        CreateSubmissionResponse response = null;
        try {
            response = (CreateSubmissionResponse)
                    RestSubmissionClient.run(getValue("spark.appResource"), mainClass, args, sparkConf,
                            new scala.collection.immutable.HashMap()
                    );
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
        return response.submissionId();
    }


    private static Map<String, String> filterSystemEnvironment(Map<String, String> env) {
        Map<String,String> map = new HashMap<>();
        for(Map.Entry<String,String> kv : env.entrySet()){
            if(kv.getKey().startsWith("SPARK_") && kv.getKey() != "SPARK_ENV_LOADED"
                    || kv.getKey().startsWith("MESOS_")){
                map.put(kv.getKey(),kv.getValue());
                log.warn("Environment has something with SPARK_ : "+kv.getKey());
            }
        }
        return map;
    }

//    public static <A, B> scala.collection.immutable.Map<A, B> toScalaMap(Map<A, B> m) {
////        return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(
////                Predef.<Tuple2<A, B>>conforms()
////        );
//        // TODO this can not be right
//        return new scala.collection.immutable.HashMap<A,B>();
//    }

}
