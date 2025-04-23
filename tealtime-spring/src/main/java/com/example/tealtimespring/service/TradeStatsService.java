package com.example.tealtimespring.service;

import com.example.tealtimespring.bean.TradeProvinceOrderAmount;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Package com.example.tealtimespring.service.TradeStatsService
 * @Author chen.jian
 * @Date 2025/4/22 16:53
 * @description: 交易
 */
public interface TradeStatsService {
    //获取某天总交易额
    BigDecimal getGMV(Integer date);

    //获取某天各个省份交易额
    List<TradeProvinceOrderAmount> getProvinceAmount(Integer date);
}
