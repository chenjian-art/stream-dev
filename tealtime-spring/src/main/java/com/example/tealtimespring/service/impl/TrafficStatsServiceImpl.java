package com.example.tealtimespring.service.impl;

import com.example.tealtimespring.bean.TrafficUvCt;
import com.example.tealtimespring.mapper.TrafficStatsMapper;
import com.example.tealtimespring.service.TrafficStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Package com.example.tealtimespring.service.impl.TrafficStatsServiceImpl
 * @Author chen.jian
 * @Date 2025/4/22 16:55
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
