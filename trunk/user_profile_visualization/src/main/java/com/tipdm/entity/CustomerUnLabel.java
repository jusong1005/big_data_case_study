package com.tipdm.entity;

import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import java.io.Serializable;

/**
 * Created by ch on 2018/11/5
 */
@Entity
@Table(name = "user_label",
        uniqueConstraints = {@UniqueConstraint(columnNames = {"phone_no", "label", "parent_label"})})
public class CustomerUnLabel<PK extends Serializable> implements Serializable {

    private static final long serialVersionUID = 5260200516679644119L;

    @EmbeddedId
    private UserLabel customerPortrayalId;

    public UserLabel getCustomerPortrayalId() {
        return customerPortrayalId;
    }

    public void setCustomerPortrayalId(UserLabel customerPortrayalId) {
        this.customerPortrayalId = customerPortrayalId;
    }
}
