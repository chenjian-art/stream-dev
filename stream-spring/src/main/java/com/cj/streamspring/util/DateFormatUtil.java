package com.cj.streamspring.util;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;

/**
 * @Package com.cj.streamspring.util.DateFormatUtil
 * @Author chen.jian
 * @Date 2025/5/2 下午2:13
 * @description:
 */
public class DateFormatUtil {
    public static Integer now(){
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }
}
