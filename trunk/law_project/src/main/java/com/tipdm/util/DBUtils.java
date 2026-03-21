package com.tipdm.util;

import com.tipdm.utils.PropertiesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * 获取数据库连接
 *
 * @Author: fansy
 * @Time: 2019/2/15 10:12
 * @Email: fansy1990@foxmail.com
 */
public class DBUtils {
    public static final String MYSQL_PROPERTIES = "mysql_result.properties";
    public static final String HBASE_PROPERTIES = "hbase_result.properties";
    public static Properties mySqlProperties = PropertiesUtil.loadProperties(MYSQL_PROPERTIES);
    public static Properties hbaseProperties = PropertiesUtil.loadProperties(HBASE_PROPERTIES);

    public static String getMySQLValues(String key) {
        return mySqlProperties.getProperty(key);
    }
    public static String getHBaseValues(String key) {
        return hbaseProperties.getProperty(key);
    }

    /**
     * 获取HBase配置
     * @return
     */
    public static Configuration getHBaseConf() {
        Configuration conf = HBaseConfiguration.create();
        //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置  
        conf.set("hbase.zookeeper.quorum", hbaseProperties.getProperty("hbase.zookeeper.quorum"));
        //设置zookeeper连接端口，默认2181  
        conf.set("hbase.zookeeper.property.clientPort", hbaseProperties.getProperty("hbase.zookeeper.clientPort"));
        return conf;
    }

    /**
     * 获取MySQL连接信息
     *
     * @return
     */
    public static Connection getMySQLConn() {
        Connection conn = null;
        try {
            // 注册 JDBC 驱动
            Class.forName(getMySQLValues("mysql.driver"));
            // 打开连接
//            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(getMySQLValues("mysql.url"),
                    getMySQLValues("mysql.username"), getMySQLValues("mysql.password"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }
}
