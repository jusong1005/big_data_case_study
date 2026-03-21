package com.tipdm.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Properties;

/**
 * Created by zhoulong on 2016/12/28.
 * E-mail:zhoulong8513@gmail.com
 */
public class PropertiesUtil {

    private final static Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);

    /**
     * @param classpath 属性文件 （classpath）
     * @return
     */
    public static Properties loadProperties(String classpath) {
        Properties properties = new Properties();
        InputStream in = null;
        try {
            in = PropertiesUtil.class.getClassLoader().getResourceAsStream(classpath);
            if(in == null){
                in = PropertiesUtil.class.getResourceAsStream(classpath);
            }
            properties.load(new InputStreamReader(in, "UTF-8"));
            return properties;

        } catch (IOException e) {
            logger.error("加载属性文件{}出错:" + e.getMessage(),classpath);
            return null;
        } finally {
            if(in != null){
                try {
                    in.close();
                } catch (IOException e) {

                }
            }
        }
    }




    private static LinkedHashMap<String, String> loadProperties(InputStream in) {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        Properties properties = new Properties();
        try {
            properties.load(new InputStreamReader(in, "UTF-8"));
        } catch (IOException e) {
            logger.error("属性文件加载出错:" + e.getMessage());
            return null;
        }
        Enumeration en = properties.propertyNames();
        while (en.hasMoreElements()) {
            String strKey = (String) en.nextElement();
            String strValue = properties.getProperty(strKey);
            map.put(strKey, strValue);
        }
        return map;
    }

    /**
     * 读取Properties的全部信息
     *
     * @param file 读取的属性文件
     * @return 返回所有的属性 key:value<>key:value
     */
    public static HashMap<String, String> getProperties(File file) {
        try {
            try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
                return loadProperties(in);
            }
        } catch (IOException e) {
            logger.error("属性文件{}加载出错:" + e.getMessage(), file.getName());
        }
        return null;
    }

    /**
     * 读取Properties的全部信息
     *
     * @param filePath 属性文件路径（classpath）
     * @return 返回所有的属性 key:value<>key:value
     */
    public static HashMap<String, String> getProperties(String filePath) {

        HashMap<String, String> map = new HashMap<>();
        Properties properties = null;
        try {
            properties = loadProperties(filePath);
        } catch (Exception e) {
            logger.error("属性文件{}加载出错:" + e.getMessage(), filePath);
            return null;
        }

        Enumeration en = properties.propertyNames();
        while (en.hasMoreElements()) {
            String strKey = (String) en.nextElement();
            String strValue = properties.getProperty(strKey);
            map.put(strKey, strValue);
        }
        return map;
    }
}
