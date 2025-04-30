package com.cj.streamspring.mapper;

import com.cj.streamspring.bean.TradeProvinceOrderAmount;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Package com.cj.streamspring.mapper.TradeStatsMapper
 * @Author chen.jian
 * @Date 2025/5/2 下午2:23
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
