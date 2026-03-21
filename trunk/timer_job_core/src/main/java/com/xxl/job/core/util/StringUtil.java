package com.xxl.job.core.util;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author: fansy
 * @Time: 2018/9/30 9:37
 * @Email: fansy1990@foxmail.com
 */
public class StringUtil {
    private static final String SINGLE_QUOTE = "\'";
    private static final String DOUBLE_QUOTE = "\"";
    public static String[] split(String line){
        List<String> list = new ArrayList<String>();
        Matcher m = Pattern.compile("([^\"]\\S*|\".+?\")\\s*").matcher(line);
        while (m.find())
            list.add(m.group(1).replaceAll("\"","")); // Add .replace("\"", "") to remove surrounding quotes.

        return list.toArray(new String[list.size()]);
    }

    public static void main(String[] args){
        String a ="1 2 3 \"A B\"";

        String[] aa = split(a);
        for(String aaa :aa){
            System.out.println(aaa);
        }
    }


}
