package com.cj.realtimespring.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.cj.realtimespring.bean.TrafficUvCt
 * @Author chen.jian
 * @Date 2025/4/18 15:24
 * @description: ss
 */

@Data
@AllArgsConstructor
public class TrafficUvCt {
    // 渠道
    String ch;
    // 独立访客数
    Integer uvCt;
}
