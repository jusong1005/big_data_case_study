package recommend;

import java.util.List;

/**
 * @Author: fansy
 * @Time: 2018/11/20 16:22
 * @Email: fansy1990@foxmail.com
 */
public class JsonModel {
    private List<String> antecedent;

    public List<String> getAntecedent() {
        return antecedent;
    }

    public void setAntecedent(List<String> antecedent) {
        this.antecedent = antecedent;
    }

    public List<String> getConsequent() {
        return consequent;
    }

    public void setConsequent(List<String> consequent) {
        this.consequent = consequent;
    }

    public double getConfidence() {
        return confidence;
    }

    public void setConfidence(double confidence) {
        this.confidence = confidence;
    }

    private List<String> consequent;
    private double confidence;
}
