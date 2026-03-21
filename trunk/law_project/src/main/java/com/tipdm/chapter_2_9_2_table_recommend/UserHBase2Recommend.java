package com.tipdm.chapter_2_9_2_table_recommend;

import com.tipdm.entity.RecEntity;
import com.tipdm.util.DBUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 使用HBase存储的数据进行推荐
 *
 * @Author: fansy
 * @Time: 2019/2/15 9:56
 * @Email: fansy1990@foxmail.com
 */
public class UserHBase2Recommend {
    public static final byte[] CF = Bytes.toBytes("recommends");
    public static final byte[] FULLURL = Bytes.toBytes("fullurl");
    public static final byte[] RATING = Bytes.toBytes("rating");

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
        List<RecEntity> recEntities = new ArrayList<>();
        Connection connection = null;
        try {
            // 获取连接
            connection = ConnectionFactory.createConnection(DBUtils.getHBaseConf());
            Admin admin = connection.getAdmin();
            TableName tableName = TableName.valueOf(DBUtils.getHBaseValues("hbase.recommend.table"));
            // 获取表引用
            Table table = connection.getTable(tableName);
            // 执行查询
            //Get
            Get theGet = new Get(Bytes.toBytes(user));
            theGet.setMaxVersions(10);// 最多推荐10个
            Result result = table.get(theGet);
            List<Cell> urls = result.getColumnCells(CF, FULLURL);
            List<Cell> ratings = result.getColumnCells(CF, RATING);

            //loop for result
            String url;
            String value;
            for (int i = 0; i < urls.size(); i++) {
                url = Bytes.toString(CellUtil.cloneValue(urls.get(i)));
                value = Bytes.toString(CellUtil.cloneValue(ratings.get(i)));
                recEntities.add(new RecEntity(url, Double.parseDouble(value)));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return recEntities;
    }

}
