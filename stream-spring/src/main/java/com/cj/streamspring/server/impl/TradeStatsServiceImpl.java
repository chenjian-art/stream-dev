package com.cj.streamspring.server.impl;

import com.cj.streamspring.bean.TradeProvinceOrderAmount;
import com.cj.streamspring.mapper.TradeStatsMapper;
import com.cj.streamspring.server.TradeStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Package com.cj.streamspring.server.impl.TradeStatsServiceImpl
 * @Author chen.jian
 * @Date 2025/5/2 下午2:27
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
