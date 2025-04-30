package com.cj.streamspring.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.cj.streamspring.bean.TradeProvinceOrderAmount
 * @Author chen.jian
 * @Date 2025/5/2 下午2:20
 * @description: 交易
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TradeProvinceOrderAmount {
    // 省份名称
    String provinceName;
    // 下单金额
    Double orderAmount;
}
