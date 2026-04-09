package com.tipdm.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.Jedis;

import java.util.*;

@RestController
@RequestMapping("/screen")
public class ScreenController {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * Redis 实时数据：订单数、营业额等
     */
    @GetMapping("/realtime")
    public Map<String, Object> getRealtimeData() {
        Map<String, Object> data = new HashMap<>();
        Jedis jedis = null;
        try {
            jedis = new Jedis("localhost", 16379, 3000);
            data.put("totalOrders", jedis.get("totalOrders"));
            data.put("totalcost", jedis.get("totalcost"));
            data.put("totalValidOrders", jedis.get("totalValidOrders"));
            data.put("increase_cost", jedis.get("increase_cost"));
            data.put("increase_order", jedis.get("increase_order"));

            // 按小时快照数据（趋势图用）
            List<Map<String, String>> hourly = new ArrayList<>();
            String[] hours = {"15", "16", "17", "18", "19", "20", "21", "22", "23"};
            for (String h : hours) {
                String key = "2026040" + (Integer.parseInt(h) < 10 ? "10" : "1") + h;
                String orders = jedis.get(key + "_totalorders");
                String cost = jedis.get(key + "_totalcost");
                if (orders != null) {
                    Map<String, String> point = new HashMap<>();
                    point.put("hour", h + ":00");
                    point.put("orders", orders);
                    point.put("cost", cost);
                    hourly.add(point);
                }
            }
            data.put("hourly", hourly);
        } catch (Exception e) {
            data.put("error", e.getMessage());
        } finally {
            if (jedis != null) jedis.close();
        }
        return data;
    }

    /**
     * MySQL 标签分布统计
     */
    @GetMapping("/label-stats")
    public List<Map<String, Object>> getLabelStats() {
        String sql = "SELECT parent_label, label, COUNT(*) as cnt FROM user_label GROUP BY parent_label, label ORDER BY parent_label, cnt DESC";
        return jdbcTemplate.queryForList(sql);
    }

    /**
     * MySQL 各大类标签用户数
     */
    @GetMapping("/parent-stats")
    public List<Map<String, Object>> getParentStats() {
        String sql = "SELECT parent_label, COUNT(*) as cnt FROM user_label GROUP BY parent_label ORDER BY cnt DESC";
        return jdbcTemplate.queryForList(sql);
    }
}
