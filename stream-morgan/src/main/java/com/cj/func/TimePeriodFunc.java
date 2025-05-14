package com.cj.func;

import java.util.Calendar;

/**
 * @Package com.cj.func.TimePeriodFunc
 * @Author chen.jian
 * @Date 2025/5/14 下午6:42
 * @description:
 */
public class TimePeriodFunc {
    public static String getPriceRange(double totalAmount) {
        if (totalAmount < 0) {
            throw new IllegalArgumentException("价格不能为负数: " + totalAmount);
        }

        if (totalAmount < 1000) {
            return "低价商品";
        } else if (totalAmount <= 4000) {
            return "中间商品";
        } else {
            return "高价商品";
        }
    }

    public static String getTimePeriod(long createTimeMillis) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(createTimeMillis);
        int hour = calendar.get(Calendar.HOUR_OF_DAY);

        if (hour >= 0 && hour < 5) {
            return "凌晨";
        } else if (hour >= 5 && hour < 8) {
            return "早晨";
        } else if (hour >= 8 && hour < 12) {
            return "上午";
        } else if (hour >= 12 && hour < 14) {
            return "中午";
        } else if (hour >= 14 && hour < 18) {
            return "下午";
        } else if (hour >= 18 && hour < 21) {
            return "晚上";
        } else {
            return "夜间";
        }
    }

    // 测试方法
    public static void main(String[] args) {
        //使用当前时间去测试
        long createTimeMillis = System.currentTimeMillis();
        String timePeriod = getTimePeriod(createTimeMillis);
        System.out.println(timePeriod);
    }
}
