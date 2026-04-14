package recommend;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.io.File;

/**
 * @Author: fansy
 * @Time: 2018/11/20 15:30
 * @Email: fansy1990@foxmail.com
 */
@Component
public class InitModel implements ApplicationListener<ContextRefreshedEvent> {

    @Value("${model.json.path}")
    private String model_json_path;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        String currPath = new File("").getAbsolutePath();
        System.out.println(currPath);
        RecommendUtils.initModel(currPath + "/" + model_json_path);
    }
}
