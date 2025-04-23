package com.example.tealtimespring.util;

import org.apache.commons.lang3.time.DateFormatUtils;
import java.util.Date;

/**
 * @Package com.example.tealtimespring.util.DateFormatUtil
 * @Author chen.jian
 * @Date 2025/4/22 16:46
 * @description: 转换
 */
public class DateFormatUtil {
    public static Integer now(){
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }
}
