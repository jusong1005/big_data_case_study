package com.tipdm.entity;


import javax.persistence.*;
import java.io.Serializable;

/**
 * Created by ch on 2018/10/30
 */
@Embeddable
public class UserLabel implements Serializable {

    private static final long serialVersionUID = 5594261560149889933L;

    @Column(name = "phone_no", nullable = false)
    private Long phone_no;
    @Column(name = "label")
    private String label;
    @Column(name = "parent_label")
    private String parent_label;

    public void setPhone_no(Long phone_no) {
        this.phone_no = phone_no;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public void setParent_label(String parent_label) {
        this.parent_label = parent_label;
    }

    public Long getPhone_no() {
        return phone_no;
    }

    public String getLabel() {
        return label;
    }

    public String getParent_label() {
        return parent_label;
    }

}
