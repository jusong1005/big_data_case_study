package com.tipdm.repository;

import com.tipdm.entity.CustomerUnLabel;
import com.tipdm.entity.UserLabel;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

/**
 * Created by ch on 2018/10/31
 */
public interface UserLabelRepository extends BaseReporitory<CustomerUnLabel, Long> {

    @Query(value = "select * from user_label where phone_no=?1 and label is  not NULL ", nativeQuery = true)
    public List<CustomerUnLabel> findByPhoneNo(Long phoneNo);
}
