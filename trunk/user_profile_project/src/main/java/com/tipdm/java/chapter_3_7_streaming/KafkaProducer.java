package com.tipdm.java.chapter_3_7_streaming;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.*;
import java.util.Properties;
/**
 * Kafka生产者
 * 程序接收一个参数：订单文件
 */
public class KafkaProducer {
//    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);
    public static void main(String[] args) {
        Properties properties = new Properties();
        InputStream in = KafkaProducer.class.getResourceAsStream("/sysconfig/kafka.properties");
        BufferedReader reader = null;
        try {
            Properties pro = new Properties();
            pro.load(new BufferedInputStream(in));
            String meta = pro.getProperty("kafka.brokers");
            String topic = pro.getProperty("kafka.topics");
            properties.put("metadata.broker.list",meta);
            properties.put("request.required.acks", "1");
            properties.put("serializer.class", "kafka.serializer.StringEncoder");
            Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(properties));
            //订单文件
            File file = new File(args[0]);
            reader = new BufferedReader(new FileReader(file));
            String line = null;
            int index =1;
            while((line = reader.readLine())!=null) {
                producer.send(new KeyedMessage<String, String>(topic,index+"",line));
                if(index%1000==0){
//                    log.info("index:"+index+"   context:"+line);
                    System.out.print("index:"+index+",content:"+line);
                }
                index +=1;
                Thread.sleep((int)(1+Math.random()*10000));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                reader.close();
                in.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
