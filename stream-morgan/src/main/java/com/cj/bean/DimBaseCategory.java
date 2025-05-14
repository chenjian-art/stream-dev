package com.cj.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Package com.cj.bean.DimBaseCategory
 * @Author chen.jian
 * @Date 2025/5/14 下午2:34
 * @description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DimBaseCategory implements Serializable {
    private String id;
    private String b3name;
    private String b2name;
    private String b1name;
}
