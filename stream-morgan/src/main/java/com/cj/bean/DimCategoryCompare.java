package com.cj.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Package com.cj.bean.DimCategoryCompare
 * @Author chen.jian
 * @Date 2025/5/14 下午2:35
 * @description:
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimCategoryCompare implements Serializable {
    private Integer id;
    private String categoryName;
    private String searchCategory;
}
