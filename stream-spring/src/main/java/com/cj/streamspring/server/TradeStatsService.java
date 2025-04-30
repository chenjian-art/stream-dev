package com.cj.streamspring.server;

import com.cj.streamspring.bean.TradeProvinceOrderAmount;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Package com.cj.streamspring.server.TradeStatsService
 * @Author chen.jian
 * @Date 2025/5/2 下午2:26
 * @description: 交易
 */
public interface TradeStatsService {
    //获取某天总交易额
    BigDecimal getGMV(Integer date);

    //获取某天各个省份交易额
    List<TradeProvinceOrderAmount> getProvinceAmount(Integer date);
}
