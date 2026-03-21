package com.tipdm.java;

import com.xxl.job.core.biz.model.ReturnT;

import java.util.Map;

/**
 * 算法实现此类，并传入参数
 * @Author: fansy
 * @Time: 2018/10/22 17:11
 * @Email: fansy1990@foxmail.com
 */
public interface JavaJobInterface {
     ReturnT<String> execute(String className, Map<String, String> args) throws Exception;
}
