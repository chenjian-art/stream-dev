package com.cj.realtimespring.service.impl;

import com.cj.realtimespring.bean.TradeProvinceOrderAmount;
import com.cj.realtimespring.mapper.TradeStatsMapper;
import com.cj.realtimespring.service.TradeStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Package com.cj.realtimespring.service.impl.TradeStatsServiceImpl
 * @Author chen.jian
 * @Date 2025/4/18 15:34
 * @description: 交易
 */

@Service
public class TradeStatsServiceImpl implements TradeStatsService {
    @Autowired
    private TradeStatsMapper tradeStatsMapper;
    @Override
    public BigDecimal getGMV(Integer date) {
        return tradeStatsMapper.selectGMV(date);
    }

    @Override
    public List<TradeProvinceOrderAmount> getProvinceAmount(Integer date) {
        return tradeStatsMapper.selectProvinceAmount(date);
    }
}
