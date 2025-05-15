package com.cj.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @Package com.cj.func.PriceTime
 * @Author chen.jian
 * @Date 2025/5/15 上午8:56
 * @description:
 */
public class PriceTime extends RichMapFunction<JSONObject, JSONObject> {
    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        String priceRange = jsonObject.getString("amount");
        String ts = jsonObject.getString("ts");
        if (priceRange.equals("高价商品")) {
            jsonObject.put("price_18_24", round(0.1 * 0.15));
            jsonObject.put("price_25_29", round(0.2 * 0.15));
            jsonObject.put("price_30_34", round(0.3 * 0.15));
            jsonObject.put("price_35_39",round( 0.4 * 0.15));
            jsonObject.put("price_40_49",round( 0.5 * 0.15));
            jsonObject.put("price_50", round(0.6 * 0.15));

        } else if (priceRange.equals("中价商品")) {
            jsonObject.put("price_18_24", round(0.2 * 0.15));
            jsonObject.put("price_25_29",round( 0.3 * 0.15));
            jsonObject.put("price_30_34",round( 0.4 * 0.15));
            jsonObject.put("price_35_39",round( 0.5 * 0.15));
            jsonObject.put("price_40_49",round( 0.6 * 0.15));
            jsonObject.put("price_50", round(0.7 * 0.15));
        } else if (priceRange.equals("低价商品")){
            jsonObject.put("price_18_24",round( 0.8 * 0.15));
            jsonObject.put("price_25_29",round( 0.6 * 0.15));
            jsonObject.put("price_30_34",round( 0.4 * 0.15));
            jsonObject.put("price_35_39",round( 0.3 * 0.15));
            jsonObject.put("price_40_49",round( 0.2 * 0.15));
            jsonObject.put("price_50", round(0.1 * 0.15));
        }
        switch (ts){
            case "凌晨":
                jsonObject.put("time_18_24",round( 0.2 * 0.1));
                jsonObject.put("time_25_29", round(0.1 * 0.1));
                jsonObject.put("time_30_34", round(0.1 * 0.1));
                jsonObject.put("time_35_39",round( 0.1 * 0.1));
                jsonObject.put("time_40_49",round( 0.1 * 0.1));
                jsonObject.put("time_50",round( 0.1 * 0.15));
                break;
            case "早晨":
                jsonObject.put("time_18_24", round(0.1 * 0.1));
                jsonObject.put("time_25_29", round(0.1 * 0.1));
                jsonObject.put("time_30_34",round( 0.1 * 0.1));
                jsonObject.put("time_35_39", round(0.1 * 0.1));
                jsonObject.put("time_40_49",round( 0.2 * 0.1));
                jsonObject.put("time_50",round( 0.2 * 0.1));
                break;
            case  "上午":
                jsonObject.put("time_18_24", round(0.2 * 0.1));
                jsonObject.put("time_25_29",round( 0.2 * 0.1));
                jsonObject.put("time_30_34",round( 0.2 * 0.1));
                jsonObject.put("time_35_39",round( 0.2 * 0.1));
                jsonObject.put("time_40_49", round(0.3 * 0.1));
                jsonObject.put("time_50", round(0.4 * 0.1));

                break;
            case "中午" :
                jsonObject.put("time_18_24",round( 0.4 * 0.1));
                jsonObject.put("time_25_29",round( 0.4 * 0.1));
                jsonObject.put("time_30_34",round( 0.4 * 0.1));
                jsonObject.put("time_35_39",round( 0.4 * 0.1));
                jsonObject.put("time_40_49",round( 0.4 * 0.1));
                jsonObject.put("time_50", round(0.3 * 0.1));

                break;
            case "下午":
                jsonObject.put("time_18_24", round(0.4 * 0.1));
                jsonObject.put("time_25_29",round( 0.5 * 0.1));
                jsonObject.put("time_30_34",round( 0.6 * 0.1));
                jsonObject.put("time_35_39",round( 0.6 * 0.1));
                jsonObject.put("time_40_49", round(0.6 * 0.1));
                jsonObject.put("time_50", round(0.4 * 0.1));


                break;
            case  "晚上":
                jsonObject.put("time_18_24", round(0.8 * 0.1));
                jsonObject.put("time_25_29", round(0.7 * 0.1));
                jsonObject.put("time_30_34", round(0.6 * 0.1));
                jsonObject.put("time_35_39", round(0.5 * 0.1));
                jsonObject.put("time_40_49",round( 0.4 * 0.1));
                jsonObject.put("time_50",round( 0.3 * 0.1));

                break;
            case  "夜间":
                jsonObject.put("time_18_24", round(0.9 * 0.1));
                jsonObject.put("time_25_29", round(0.7 * 0.1));
                jsonObject.put("time_30_34", round(0.5 * 0.1));
                jsonObject.put("time_35_39", round(0.3 * 0.1));
                jsonObject.put("time_40_49", round(0.2 * 0.1));
                jsonObject.put("time_50", round(0.1 * 0.1));
        }
        return jsonObject;
    }
    private static double round(double value) {
        return BigDecimal.valueOf(value)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }
}
