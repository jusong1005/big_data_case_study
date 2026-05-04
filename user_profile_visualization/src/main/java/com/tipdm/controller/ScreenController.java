package com.tipdm.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

@RestController
@RequestMapping({"/screen", "/api/screen"})
public class ScreenController {

    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private StringRedisTemplate redisTemplate;

    /**
     * Redis 实时数据：订单数、营业额等
     */
    @GetMapping("/realtime")
    public Map<String, Object> getRealtimeData() {
        Map<String, Object> data = new HashMap<>();
        try {
            data.put("totalOrders", redisTemplate.opsForValue().get("totalOrders"));
            data.put("totalcost", redisTemplate.opsForValue().get("totalcost"));
            data.put("totalValidOrders", redisTemplate.opsForValue().get("totalValidOrders"));
            data.put("increase_cost", redisTemplate.opsForValue().get("increase_cost"));
            data.put("increase_order", redisTemplate.opsForValue().get("increase_order"));

            // 按小时快照数据（趋势图用）
            List<Map<String, String>> hourly = new ArrayList<>();
            String[] hours = {"15", "16", "17", "18", "19", "20", "21", "22", "23"};
            for (String h : hours) {
                String key = "2026040" + (Integer.parseInt(h) < 10 ? "10" : "1") + h;
                String orders = redisTemplate.opsForValue().get(key + "_totalorders");
                String cost = redisTemplate.opsForValue().get(key + "_totalcost");
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

    @GetMapping("/model-metrics")
    public Map<String, Object> getModelMetrics() {
        String sql = "SELECT param_original, value FROM user_profile_svm_metrics ORDER BY param_original";
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);
        Map<String, Object> metrics = new LinkedHashMap<>();
        for (Map<String, Object> row : rows) {
            metrics.put(String.valueOf(row.get("param_original")), row.get("value"));
        }
        return metrics;
    }

    @GetMapping("/summary")
    public Map<String, Object> getSummary() {
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("totalUsers", jdbcTemplate.queryForObject("SELECT COUNT(DISTINCT phone_no) FROM user_label", Long.class));
        summary.put("totalLabels", jdbcTemplate.queryForObject("SELECT COUNT(*) FROM user_label", Long.class));
        summary.put("labelCategoryCount", jdbcTemplate.queryForObject("SELECT COUNT(DISTINCT parent_label) FROM user_label", Long.class));
        summary.put("metrics", getModelMetrics());
        return summary;
    }
}
