package com.cj.realtimespring.service;

import com.cj.realtimespring.bean.TradeProvinceOrderAmount;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Package com.cj.realtimespring.service.impl.TradeStatsService
 * @Author chen.jian
 * @Date 2025/4/18 15:33
 * @description: 交易
 *
 */

public interface TradeStatsService {

    //获取某天总交易额
    BigDecimal getGMV(Integer date);

    //获取某天各个省份交易额
    List<TradeProvinceOrderAmount> getProvinceAmount(Integer date);
}
