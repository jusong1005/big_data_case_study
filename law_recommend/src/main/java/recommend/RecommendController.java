package recommend;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @Author: fansy
 * @Time: 2018/11/20 15:05
 * @Email: fansy1990@foxmail.com
 */
@RestController
public class RecommendController {
    @RequestMapping("/rec")
    @ResponseBody
    public Info greeting(@RequestParam(value = "visits") String visits) {
        List<String> visitsUrl = Arrays.asList(visits.split(","));
        List<RecModel> recs = RecommendUtils.rec(visitsUrl);
        return new Info(new Random().toString(), visitsUrl, recs);
    }
}
