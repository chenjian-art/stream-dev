package com.cj.base;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Package com.cj.base.DimSkuInfoMsg
 * @Author chen.jian
 * @Date 2025/5/15 下午4:44
 * @description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DimSkuInfoMsg implements Serializable {
    private String skuid;
    private String spuid;
    private String c3id;
    private String tname;
}
