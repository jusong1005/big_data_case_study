package recommend;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 推荐工具类
 *
 * @Author: fansy
 * @Time: 2018/11/20 15:15
 * @Email: fansy1990@foxmail.com
 */
@Component
public class RecommendUtils {
    private static Logger log = LoggerFactory.getLogger(RecommendUtils.class);
    private static List<JsonModel> jsonModelList;

    /**
     * 确认是否匹配
     *
     * @param rule
     * @param visits
     * @return
     */
    private static boolean match(List<String> rule, List<String> visits) {
        int sum = 0;
        for (String r : rule) {
            if (visits.contains(r)) {
                sum++;
            }
        }
        return sum == rule.size();
    }

    /**
     * 推荐
     * 一个visits过来后，如果这个visits包含了某条规则的antecedent，那么就进行输出，最后按照confidence排序：
     * antecedent|consequent|        confidence
     *
     * @param visits
     * @return
     */
    public static List<RecModel> rec(List<String> visits) {
        List<RecModel> recs = new ArrayList<>();
        for (JsonModel rule : jsonModelList) {
            if (match(rule.getAntecedent(), visits)) {
                for (String conse : rule.getConsequent()) {
                    if (!visits.contains(conse)) {
                        recs.add(new RecModel(conse, rule.getConfidence()));
                    }
                }
            }
        }
        Collections.sort(recs);
        return recs;
    }

    /**
     * json字符串到Java类
     *
     * @param jsonFile
     */
    public static void initModel(String jsonFile) {
        String jsonStr = read2Json(jsonFile);
        jsonModelList = JSON.parseArray(jsonStr.toString(), JsonModel.class);
        log.info("模型初始化完成!");
    }

    /**
     * 读取json文件
     *
     * @param jsonFile
     * @return
     */
    private static String read2Json(String jsonFile) {
        BufferedReader reader = null;
        StringBuilder lines = new StringBuilder();
        lines.append("[");
        try {
            FileInputStream fileInputStream = new FileInputStream(jsonFile);
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, "UTF-8");
            reader = new BufferedReader(inputStreamReader);
            String tempString = null;
            while ((tempString = reader.readLine()) != null) {
                lines.append(tempString);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        lines.append("]");
        return lines.toString();
    }

    public static void main(String[] args) {
        String jsonFile = "C:/Users/fansy/Downloads/part-00000-eea69fea-7fd2-4f99-9df3-7c9d33dd70aa-c000.json";
        String jsonStr = read2Json(jsonFile);
        List<JsonModel> list = JSON.parseArray(jsonStr.toString(), JsonModel.class);

        System.out.println(list);
    }

}
