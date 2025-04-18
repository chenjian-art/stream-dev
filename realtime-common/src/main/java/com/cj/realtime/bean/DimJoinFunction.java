package com.cj.realtime.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * @Package com.cj.realtime.bean.DimJoinFunction
 * @Author chen.jian
 * @Date 2025/4/15 11:05
 * @description: 接口
 */
public interface DimJoinFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj) ;

    String getTableName() ;

    String getRowKey(T obj) ;
}
