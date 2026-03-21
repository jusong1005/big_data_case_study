package com.tipdm.chapter_2_9_2_table_recommend;

import com.tipdm.entity.RecEntity;
import com.tipdm.util.DBUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 使用MySQL存储的数据进行推荐
 *
 * @Author: fansy
 * @Time: 2019/2/15 9:56
 * @Email: fansy1990@foxmail.com
 */
public class UserMySQL2Recommend {
    public static void main(String[] args) {
        String user = "1034440007.1403268321";
        List<RecEntity> recEntities = getRecommend(user);
        Collections.sort(recEntities);
        System.out.println("对用户：" + user + " 的推荐结果：");
        for (RecEntity entity : recEntities) {
            System.out.println(entity);
        }
    }

    /**
     * 推荐
     *
     * @param user
     * @return
     */
    public static List<RecEntity> getRecommend(String user) {
        List<RecEntity> result = new ArrayList<>();
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = DBUtils.getMySQLConn();

            // 执行查询
//            System.out.println(" 实例化Statement对象...");
            stmt = conn.createStatement();
            String sql;
            sql = "SELECT userid, fullurl, rating FROM " +
                    DBUtils.getMySQLValues("mysql.recommend.table") +
                    " WHERE userid = '" + user + "'";
            ResultSet rs = stmt.executeQuery(sql);
            // 展开结果集数据库
            String url;
            double value;
            while (rs.next()) {
                // 通过字段检索
                url = rs.getString("fullurl");
                value = rs.getDouble("rating");
                result.add(new RecEntity(url, value));
            }
            // 完成后关闭
            rs.close();
            stmt.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            try {
                stmt.close();
                conn.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result;
    }

}
