package com.example.tealtimespring.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.example.tealtimespring.bean.TrafficUvCt
 * @Author chen.jian
 * @Date 2025/4/22 16:48
 * @description: bb
 */
@Data
@AllArgsConstructor
public class TrafficUvCt {
    // 渠道
    String ch;
    // 独立访客数
    Integer uvCt;
}
