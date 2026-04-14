package com.tipdm.controller;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.Serializable;

/**
 * Created by ch on 2018/11/5
 */
@JsonSerialize
public class Result implements Serializable {

    private static final long serialVersionUID = 3285893257649120235L;
    private String message = "操作成功";
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private Object data;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    private Status status = Status.SUCCESS;

    public enum Status {
        SUCCESS, FAIL
    }


}
