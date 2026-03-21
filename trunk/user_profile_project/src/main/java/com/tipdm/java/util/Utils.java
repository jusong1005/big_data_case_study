package com.tipdm.java.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ch on 2018/9/10
 */
public class Utils {
    private static String path = Utils.class.getClassLoader().getResource("rules/consume_content.json").getPath();

    /**
     *
     * @param label：二级标签名称
     * @return：拼接标签的when语句
     * @throws IOException
     */
    public static String getSQL(String label) throws IOException {
        File file = new File(path);
        String json = FileUtils.readFileToString(file);
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
