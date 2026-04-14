package recommend;

import java.util.List;

/**
 * @Author: fansy
 * @Time: 2018/11/20 15:04
 * @Email: fansy1990@foxmail.com
 */
public class Info {
    private String name;

    private List<String> visits;

    private List<RecModel> recommends;

    public Info() {
    }

    public Info(String name, List<String> visits, List<RecModel> recommends) {
        this.name = name;
        this.visits = visits;
        this.recommends = recommends;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<RecModel> getRecommends() {
        return recommends;
    }

    public void setRecommends(List<RecModel> recommends) {
        this.recommends = recommends;
    }

    public List<String> getVisits() {
        return visits;
    }

    public void setVisits(List<String> visits) {
        this.visits = visits;
    }
}
