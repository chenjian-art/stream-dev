package com.cj.streamspring.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.cj.streamspring.bean.TrafficUvCt
 * @Author chen.jian
 * @Date 2025/5/2 下午2:20
 * @description: 流量
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficUvCt {
    // 渠道
    String ch;
    // 独立访客数
    Integer uvCt;
}
