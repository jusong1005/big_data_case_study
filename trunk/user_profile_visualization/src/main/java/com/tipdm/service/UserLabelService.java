package com.tipdm.service;

import com.tipdm.common.LabelNames;
import com.tipdm.entity.CustomerUnLabel;
import com.tipdm.entity.UserLabel;
import com.tipdm.repository.UserLabelRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

/**
 * Created by ch on 2018/10/31
 */
@Service
@Transactional
@PropertySource("classpath:application.yml")
public class UserLabelService {
    @Autowired
    private UserLabelRepository userLabelRepository;
    @Autowired
    private LabelNames labelNames;
    @Value("${number}")
    private int number;
    public List getLabelsByPhoneNo(Long phoneNo) {
        List<CustomerUnLabel> userLabels = userLabelRepository.findByPhoneNo(phoneNo);
        List<UserLabel> useLabelLists = new ArrayList<>();
        for (CustomerUnLabel userLabel : userLabels) {
            UserLabel label = userLabel.getCustomerPortrayalId();
            useLabelLists.add(label);
        }
        return useLabelLists;
    }
    public Map<String, Object> findLabel(Long phoneNo) {
        List<String> parentLabels = labelNames.getLabelNames();
        List<CustomerUnLabel> userLabels = userLabelRepository.findByPhoneNo(phoneNo);
        int size = userLabels.size();
        Map<String, Object> map = new HashMap<>();
        List<String> parent = new ArrayList<>();
        for (int i = 0; i < number; i++) {
            parent.add(parentLabels.get(i));
            List<String> child = new ArrayList<>();
            for (CustomerUnLabel userLabel : userLabels) {
                UserLabel label = userLabel.getCustomerPortrayalId();
                if (parentLabels.get(i).equals(label.getParent_label())) {
                    child.add(label.getLabel());
                }
            }
            map.put(parentLabels.get(i), child);
        }
        map.put("isNull", size == 0);
        map.put("parentName", parent);
        return map;
    }
}

