package com.cj.realtime;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.cj.realtime.Emp
 * @Author chen.jian
 * @Date 2025/4/10 16:28
 * @description: 6
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Emp {
    Integer empno;
    String ename;
    Integer deptno;
    Long ts;
}
