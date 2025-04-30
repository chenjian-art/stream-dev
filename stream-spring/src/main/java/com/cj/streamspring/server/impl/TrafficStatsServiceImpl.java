package com.cj.streamspring.server.impl;

import com.cj.streamspring.bean.TrafficUvCt;
import com.cj.streamspring.mapper.TrafficStatsMapper;
import com.cj.streamspring.server.TrafficStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Package com.cj.streamspring.server.impl.TrafficStatsServiceImpl
 * @Author chen.jian
 * @Date 2025/5/2 下午2:28
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
