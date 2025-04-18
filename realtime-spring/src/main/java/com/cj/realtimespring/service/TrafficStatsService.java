package com.cj.realtimespring.service;

import com.cj.realtimespring.bean.TrafficUvCt;

import java.util.List;

/**
 * @Package com.cj.realtimespring.service.TrafficStatsService
 * @Author chen.jian
 * @Date 2025/4/18 18:38
 * @description: 流量
 */
public interface TrafficStatsService {

    List<TrafficUvCt> getChUvC(Integer date,Integer limit);
}
