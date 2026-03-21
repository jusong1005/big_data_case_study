package com.tipdm.common;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Created by ch on 2018/11/5
 */
@Component
@ConfigurationProperties(prefix = "labelName")
public class LabelNames {
    private List<String> labelNames;

    public void setLabelNames(List<String> labelNames) {
        this.labelNames = labelNames;
    }

    public List<String> getLabelNames() {
        return labelNames;
    }
}
