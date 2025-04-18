package com.cj.realtimespring.service.impl;

import com.cj.realtimespring.bean.TrafficUvCt;
import com.cj.realtimespring.mapper.TradeStatsMapper;
import com.cj.realtimespring.mapper.TrafficStatsMapper;
import com.cj.realtimespring.service.TrafficStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Package com.cj.realtimespring.service.impl.TrafficStatsServiceImpl
 * @Author chen.jian
 * @Date 2025/4/18 18:39
 * @description: 流量
 */
@Service
public class TrafficStatsServiceImpl implements TrafficStatsService {
    @Autowired
    public TrafficStatsMapper trafficStatsMapper;
    @Override
    public List<TrafficUvCt> getChUvC(Integer date, Integer limit) {
        return trafficStatsMapper.selectChUvCt(date,limit);
    }
}
