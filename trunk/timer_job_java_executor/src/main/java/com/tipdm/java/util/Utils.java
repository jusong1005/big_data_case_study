package com.tipdm.java.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ch on 2018/9/10
 */
public class Utils {
    //打成jar后运行，结果不能读取到文件。在jar里面对应的class路径下可以看到该文件，确定是有打包进去的。
    //此时通过 this.getClass().getResource("")；方法无法正确获取文件。

    public static String getSQL(String label) throws IOException {
        InputStream is = Utils.class.getClassLoader().getResourceAsStream("rules/consume_content.json");
        StringWriter writer = new StringWriter();
        IOUtils.copy(is, writer, "UTF-8");
        String json = writer.toString();
        JSONObject jsonObject = JSONObject.parseObject(json);
        JSONArray jsonArray = jsonObject.getJSONArray(label);
        StringBuilder sql = new StringBuilder();
        if (jsonArray.size() > 0) {
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject obj = jsonArray.getJSONObject(i);
                String rules = (String) obj.get("rules");
                sql.append("when").append(" ").append(rules).append(" ");
                String la = (String) obj.get("label");
                Pattern pattern = Pattern.compile("[\\u4e00-\\u9fa5]");
                Matcher matcher = pattern.matcher(la.charAt(0) + "");
                if (matcher.matches()) {
                    sql.append("then ").append("\"" + la + "\"" + " ");
                } else {
                    sql.append("then ").append(la + " ");
                }
            }
            sql.append(" end as label,\"" + label + "\" as parent_label");
        } else {
            System.out.println("rules/consume_content.json文件中没有此标签");
            System.exit(1);
        }
        return sql.toString();
    }
}
