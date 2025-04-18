package com.cj.realtimespring.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.cj.realtimespring.bean.TradeProvinceOrderAmount
 * @Author chen.jian
 * @Date 2025/4/18 15:23
 * @description: aa
 */

@Data
@AllArgsConstructor
public class TradeProvinceOrderAmount {
    // 省份名称
    String provinceName;
    // 下单金额
    Double orderAmount;
}
