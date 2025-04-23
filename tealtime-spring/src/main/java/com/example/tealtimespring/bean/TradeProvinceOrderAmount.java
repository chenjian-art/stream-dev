package com.example.tealtimespring.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.example.tealtimespring.bean.TradeProvinceOrderAmount
 * @Author chen.jian
 * @Date 2025/4/22 16:47
 * @description: aa
 */
@Data
@AllArgsConstructor
public class TradeProvinceOrderAmount{
    // 省份名称
    String provinceName;
    // 下单金额
    Double orderAmount;
}
