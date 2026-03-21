package com.tipdm.entity;

/**
 * 表推荐 封装实体
 * @Author: fansy
 * @Time: 2019/2/15 10:08
 * @Email: fansy1990@foxmail.com
 */
public class RecEntity implements Comparable<RecEntity> {
    private String url;
    private Double value;

    public RecEntity(){}
    public RecEntity(String url, Double value){
        this.url = url;
        this.value = value;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "[" + this.url + " : " + this.value + "]";
    }

    @Override
    public int compareTo(RecEntity o) {
        return -this.value.compareTo(o.value);
    }
}
