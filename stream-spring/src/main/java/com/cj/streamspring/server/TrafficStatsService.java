package com.cj.streamspring.server;

import com.cj.streamspring.bean.TrafficUvCt;

import java.util.List;

/**
 * @Package com.cj.streamspring.server.TrafficStatsService
 * @Author chen.jian
 * @Date 2025/5/2 下午2:26
 * @description: 流量
 */
public interface TrafficStatsService {
    List<TrafficUvCt> getChUvC(Integer date, Integer limit);
}
