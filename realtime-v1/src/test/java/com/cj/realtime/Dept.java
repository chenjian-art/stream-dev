package com.cj.realtime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.cj.realtime.Dept
 * @Author chen.jian
 * @Date 2025/4/10 16:29
 * @description: 7
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Dept {
    Integer deptno;
    String dname;
    Long ts;
}
