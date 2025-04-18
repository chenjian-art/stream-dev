package com.cj.realtimespring.mapper;

import com.cj.realtimespring.bean.TradeProvinceOrderAmount;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Package com.cj.realtimespring.mapper.TradeStatsMapper
 * @Author chen.jian
 * @Date 2025/4/18 15:28
 * @description: 交易
 */
@Mapper
public interface TradeStatsMapper {
    //获取某天总交易额
    @Select("select sum(order_amount) order_amount from dws_trade_province_order_window partition par#{date}")
    BigDecimal selectGMV(Integer date);

    //获取某天各个省份交易额
    @Select("select province_name,sum(order_amount) order_amount from dws_trade_province_order_window partition par#{date} " +
            " GROUP BY province_name")
    List<TradeProvinceOrderAmount> selectProvinceAmount(Integer date);
}
