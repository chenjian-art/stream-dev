package com.example.tealtimespring.service;

import com.example.tealtimespring.bean.TrafficUvCt;

import java.util.List;

/**
 * @Package com.example.tealtimespring.service.TrafficStatsService
 * @Author chen.jian
 * @Date 2025/4/22 16:53
 * @description: 流量
 */
public interface TrafficStatsService {
    List<TrafficUvCt> getChUvC(Integer date, Integer limit);
}
