package recommend;

/**
 * @Author: fansy
 * @Time: 2018/11/20 16:23
 * @Email: fansy1990@foxmail.com
 */
public class RecModel implements Comparable<RecModel> {
    private String consequent;
    private double confidence;

    public String getConsequent() {
        return consequent;
    }

    public void setConsequent(String consequent) {
        this.consequent = consequent;
    }

    public double getConfidence() {
        return confidence;
    }

    public void setConfidence(double confidence) {
        this.confidence = confidence;
    }

    public RecModel() {
    }

    public RecModel(String consequent, double confidence) {
        this.consequent = consequent;
        this.confidence = confidence;
    }

    @Override
    public int compareTo(RecModel o) {
        if (this.confidence > o.confidence) {
            return -1;
        } else if (this.confidence < o.confidence) {
            return 1;
        }
        return this.consequent.compareTo(o.consequent);
    }
}
