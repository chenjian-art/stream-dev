package com.cj.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.doris.shaded.org.apache.arrow.flatbuf.Int;
import org.apache.flink.api.common.functions.RichMapFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @Package com.cj.func.pricebase
 * @Author chen.jian
 * @Date 2025/5/15 上午9:23
 * @description:
 */
public class pricebase extends RichMapFunction<JSONObject, JSONObject> {
    private double roundToThreeDecimals(double value) {
        return new BigDecimal(value)
                .setScale(3, RoundingMode.HALF_UP)
                .doubleValue();
    }

    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        JSONObject object = new JSONObject();
        Integer age = jsonObject.getInteger("age");
        // 类目
        Set<String> TrendCategoryValues = new HashSet<>(Arrays.asList("珠宝", "礼品箱包", "鞋靴", "服饰内衣", "个护化妆", "数码"));
        Set<String> HomeCategoryValues = new HashSet<>(Arrays.asList("母婴", "钟表", "厨具", "电脑办公", "家居家装", "家用电器", "图书、音像、电子书刊", "手机", "汽车用品"));
        Set<String> HealthCategoryValues = new HashSet<>(Arrays.asList("运动健康", "食品饮料、保健食品"));
        String category1Name = jsonObject.getString("category1_name");
        //品牌
        Set<String> EquipmentTmValues = new HashSet<>(Arrays.asList("Redmi", "苹果", "联想", "TCL", "小米"));
        Set<String> TrendTmValues = new HashSet<>(Arrays.asList("长粒香", "金沙河", "索芙特", "CAREMiLLE", "欧莱雅", "香奈儿"));
        String tmName = jsonObject.getString("tm_name");
        //时间
        String ts = jsonObject.getString("ts");
        //价格
        String amount = jsonObject.getString("amount");
        //设备
        String os = jsonObject.getString("judge_os");
        // 搜索词
        String keywordType = jsonObject.getString("search_category");

        if (age != null && age>=18 && age<=24){

            //判断品类
            if (TrendCategoryValues.contains(category1Name)){
                object.put("category_score", roundToThreeDecimals(0.9 * 0.3));
            } else if (HomeCategoryValues.contains(category1Name)) {
                object.put("category_score", roundToThreeDecimals(0.2 * 0.3));
            } else if (HealthCategoryValues.contains(category1Name)) {
                object.put("category_score", roundToThreeDecimals(0.1 * 0.3));
            }
            //判断品牌
            if (EquipmentTmValues.contains(tmName)){
                object.put("tm_score", roundToThreeDecimals(0.9 * 0.2));
            }else if (TrendTmValues.contains(tmName)){
                object.put("tm_score", roundToThreeDecimals(0.1 * 0.2));
            }
            // 判断价格敏感度
            switch (amount) {
                case "高价商品":
                    object.put("price_sensitive_score", roundToThreeDecimals(0.1 * 0.15));
                    break;
                case "中价商品":
                    object.put("price_sensitive_score", roundToThreeDecimals(0.2 * 0.15));
                    break;
                case "低价商品":
                    object.put("price_sensitive_score", roundToThreeDecimals(0.8 * 0.15));
                    break;
            }
            // 判断时间段
            switch (ts) {
                case "凌晨":
                case "上午":
                    object.put("time_period_score", roundToThreeDecimals(0.2 * 0.1));
                    break;
                case "下午":
                case "中午":
                    object.put("time_period_score", roundToThreeDecimals(0.4 * 0.1));
                    break;
                case "早晨":
                    object.put("time_period_score", roundToThreeDecimals(0.1 * 0.1));
                    break;
                case "晚上":
                    object.put("time_period_score", roundToThreeDecimals(0.8 * 0.1));
                    break;
                case "夜间":
                    object.put("time_period_score", roundToThreeDecimals(0.9 * 0.1));
                    break;
            }
            // 判断搜索词
            switch (keywordType) {
                case " 时尚与潮流 ":
                    object.put("keyword_type_score", roundToThreeDecimals(0.9 * 0.15));
                    break;
                case "性价比":
                    object.put("keyword_type_score", roundToThreeDecimals(0.2 * 0.15));
                    break;
                case "健康与养生":
                case "家庭与育儿":
                    object.put("keyword_type_score", roundToThreeDecimals(0.1 * 0.15));
                    break;
                case "科技与数码":
                    object.put("keyword_type_score", roundToThreeDecimals(0.8 * 0.15));
                    break;
                case "学习与发展":
                    object.put("keyword_type_score", roundToThreeDecimals(0.4 * 0.15));
                    break;
            }
            // 判断设备信息
            if (os.equals("Android")){
                object.put("os_score", roundToThreeDecimals(0.8 * 0.1));
            } else if (os.equals("iOS")) {
                object.put("os_score", roundToThreeDecimals(0.7 * 0.1));
            }

        }else if (age != null && age>=25 && age<=29){

            //判断品类
            if (TrendCategoryValues.contains(category1Name)){
                object.put("category_score", roundToThreeDecimals(0.8 * 0.3));
            } else if (HomeCategoryValues.contains(category1Name)) {
                object.put("category_score", roundToThreeDecimals(0.4 * 0.3));
            } else if (HealthCategoryValues.contains(category1Name)) {
                object.put("category_score", roundToThreeDecimals(0.2 * 0.3));
            }
            //判断品牌
            if (EquipmentTmValues.contains(tmName)){
                object.put("tm_score", roundToThreeDecimals(0.7 * 0.2));
            }else if (TrendTmValues.contains(tmName)){
                object.put("tm_score", roundToThreeDecimals(0.3 * 0.2));
            }
            // 判断价格敏感度
            switch (amount) {
                case "高价商品":
                    object.put("price_sensitive_score", roundToThreeDecimals(0.2 * 0.15));
                    break;
                case "中价商品":
                    object.put("price_sensitive_score", roundToThreeDecimals(0.4 * 0.15));
                    break;
                case "低价商品":
                    object.put("price_sensitive_score", roundToThreeDecimals(0.6 * 0.15));
                    break;
            }

            // 判断时间段
            switch (ts) {
                case "凌晨":
                case "早晨":
                    object.put("time_period_score", roundToThreeDecimals(0.1 * 0.1));
                    break;
                case "上午":
                    object.put("time_period_score", roundToThreeDecimals(0.2 * 0.1));
                    break;
                case "中午":
                    object.put("time_period_score", roundToThreeDecimals(0.4 * 0.1));
                    break;
                case "下午":
                    object.put("time_period_score", roundToThreeDecimals(0.5 * 0.1));
                    break;
                case "晚上":
                case "夜间":
                    object.put("time_period_score", roundToThreeDecimals(0.7 * 0.1));
                    break;
            }
            // 判断搜索词
            switch (keywordType) {
                case " 时尚与潮流 ":
                    object.put("keyword_type_score", roundToThreeDecimals(0.7 * 0.15));
                    break;
                case "性价比":
                    object.put("keyword_type_score", roundToThreeDecimals(0.4 * 0.15));
                    break;
                case "健康与养生":
                case "家庭与育儿":
                    object.put("keyword_type_score", roundToThreeDecimals(0.2 * 0.15));
                    break;
                case "科技与数码":
                    object.put("keyword_type_score", roundToThreeDecimals(0.6 * 0.15));
                    break;
                case "学习与发展":
                    object.put("keyword_type_score", roundToThreeDecimals(0.5 * 0.15));
                    break;
            }
            // 判断设备信息
            if (os.equals("Android")){
                object.put("os_score", roundToThreeDecimals(0.7 * 0.1));
            } else if (os.equals("iOS")) {
                object.put("os_score", roundToThreeDecimals(0.6 * 0.1));
            }
        }else if (age != null && age>=30 && age<=34) {

            //判断品类
            if (TrendCategoryValues.contains(category1Name)){
                object.put("category_score", roundToThreeDecimals(0.6 * 0.3));
            } else if (HomeCategoryValues.contains(category1Name)) {
                object.put("category_score", roundToThreeDecimals(0.6 * 0.3));
            } else if (HealthCategoryValues.contains(category1Name)) {
                object.put("category_score", roundToThreeDecimals(0.4 * 0.3));
            }
            //判断品牌
            if (EquipmentTmValues.contains(tmName)){
                object.put("tm_score", roundToThreeDecimals(0.5 * 0.2));
            }else if (TrendTmValues.contains(tmName)){
                object.put("tm_score", roundToThreeDecimals(0.5 * 0.2));
            }
            // 判断价格敏感度
            switch (amount) {
                case "高价商品":
                    object.put("price_sensitive_score", roundToThreeDecimals(0.3 * 0.15));
                    break;
                case "中价商品":
                    object.put("price_sensitive_score", roundToThreeDecimals(0.6 * 0.15));
                    break;
                case "低价商品":
                    object.put("price_sensitive_score", roundToThreeDecimals(0.4 * 0.15));
                    break;
            }

            // 判断时间段
            switch (ts) {
                case "凌晨":
                case "早晨":
                    object.put("time_period_score", roundToThreeDecimals(0.1 * 0.1));
                    break;
                case "上午":
                    object.put("time_period_score", roundToThreeDecimals(0.2 * 0.1));
                    break;
                case "中午":
                    object.put("time_period_score", roundToThreeDecimals(0.4 * 0.1));
                    break;
                case "下午":
                case "夜间":
                    object.put("time_period_score", roundToThreeDecimals(0.5 * 0.1));
                    break;
                case "晚上":
                    object.put("time_period_score", roundToThreeDecimals(0.6 * 0.1));
                    break;
            }
            // 判断搜索词
            switch (keywordType) {
                case " 时尚与潮流 ":
                    object.put("keyword_type_score", roundToThreeDecimals(0.5 * 0.15));
                    break;
                case "性价比":
                case "学习与发展":
                    object.put("keyword_type_score", roundToThreeDecimals(0.6 * 0.15));
                    break;
                case "健康与养生":
                case "家庭与育儿":
                case "科技与数码":
                    object.put("keyword_type_score", roundToThreeDecimals(0.4 * 0.15));
                    break;
            }
            // 判断设备信息
            if (os.equals("Android")){
                object.put("os_score", roundToThreeDecimals(0.6 * 0.1));
            } else if (os.equals("iOS")) {
                object.put("os_score", roundToThreeDecimals(0.5 * 0.1));
            }
        }else if (age != null && age>=35 && age<=39){

            //判断品类
            if (TrendCategoryValues.contains(category1Name)){
                object.put("category_score", roundToThreeDecimals(0.4 * 0.3));
            } else if (HomeCategoryValues.contains(category1Name)) {
                object.put("category_score", roundToThreeDecimals(0.8 * 0.3));
            } else if (HealthCategoryValues.contains(category1Name)) {
                object.put("category_score", roundToThreeDecimals(0.6 * 0.3));
            }
            //判断品牌
            if (EquipmentTmValues.contains(tmName)){
                object.put("tm_score", roundToThreeDecimals(0.3 * 0.2));
            }else if (TrendTmValues.contains(tmName)){
                object.put("tm_score", roundToThreeDecimals(0.7 * 0.2));
            }
            // 判断价格敏感度
            switch (amount) {
                case "高价商品":
                    object.put("price_sensitive_score", roundToThreeDecimals(0.4 * 0.15));
                    break;
                case "中价商品":
                    object.put("price_sensitive_score", roundToThreeDecimals(0.7 * 0.15));
                    break;
                case "低价商品":
                    object.put("price_sensitive_score", roundToThreeDecimals(0.3 * 0.15));
                    break;
            }

            // 判断时间段
            switch (ts) {
                case "凌晨":
                case "早晨":
                    object.put("time_period_score", roundToThreeDecimals(0.1 * 0.1));
                    break;
                case "上午":
                    object.put("time_period_score", roundToThreeDecimals(0.2 * 0.1));
                    break;
                case "中午":
                    object.put("time_period_score", roundToThreeDecimals(0.4 * 0.1));
                    break;
                case "下午":
                case "晚上":
                    object.put("time_period_score", roundToThreeDecimals(0.5 * 0.1));
                    break;
                case "夜间":
                    object.put("time_period_score", roundToThreeDecimals(0.3 * 0.1));
                    break;
            }
            // 判断搜索词
            switch (keywordType) {
                case " 时尚与潮流 ":
                case "科技与数码":
                    object.put("keyword_type_score", roundToThreeDecimals(0.3 * 0.15));
                    break;
                case "性价比":
                case "学习与发展":
                    object.put("keyword_type_score", roundToThreeDecimals(0.7 * 0.15));
                    break;
                case "健康与养生":
                case "家庭与育儿":
                    object.put("keyword_type_score", roundToThreeDecimals(0.6 * 0.15));
                    break;
            }
            // 判断设备信息
            if (os.equals("Android")){
                object.put("os_score", roundToThreeDecimals(0.5 * 0.1));
            } else if (os.equals("iOS")) {
                object.put("os_score", roundToThreeDecimals(0.4 * 0.1));
            }
        }else if (age != null && age>=40 && age<=49){

            //判断品类
            if (TrendCategoryValues.contains(category1Name)){
                object.put("category_score", roundToThreeDecimals(0.2 * 0.3));
            } else if (HomeCategoryValues.contains(category1Name)) {
                object.put("category_score", roundToThreeDecimals(0.9 * 0.3));
            } else if (HealthCategoryValues.contains(category1Name)) {
                object.put("category_score", roundToThreeDecimals(0.8 * 0.3));
            }
            //判断品牌
            if (EquipmentTmValues.contains(tmName)){
                object.put("tm_score", roundToThreeDecimals(0.2 * 0.2));
            }else if (TrendTmValues.contains(tmName)){
                object.put("tm_score", roundToThreeDecimals(0.8 * 0.2));
            }
            // 判断价格敏感度
            switch (amount) {
                case "高价商品":
                    object.put("price_sensitive_score", roundToThreeDecimals(0.5 * 0.15));
                    break;
                case "中价商品":
                    object.put("price_sensitive_score", roundToThreeDecimals(0.8 * 0.15));
                    break;
                case "低价商品":
                    object.put("price_sensitive_score", roundToThreeDecimals(0.2 * 0.15));
                    break;
            }

            // 判断时间段
            switch (ts) {
                case "凌晨":
                    object.put("time_period_score", roundToThreeDecimals(0.1 * 0.1));
                    break;
                case "早晨":
                case "夜间":
                    object.put("time_period_score", roundToThreeDecimals(0.2 * 0.1));
                    break;
                case "上午":
                    object.put("time_period_score", roundToThreeDecimals(0.3 * 0.1));
                    break;
                case "中午":
                case "晚上":
                    object.put("time_period_score", roundToThreeDecimals(0.4 * 0.1));
                    break;
                case "下午":
                    object.put("time_period_score", roundToThreeDecimals(0.5 * 0.1));
                    break;
            }
            // 判断搜索词
            switch (keywordType) {
                case " 时尚与潮流 ":
                case "科技与数码":
                    object.put("keyword_type_score", roundToThreeDecimals(0.2 * 0.15));
                    break;
                case "性价比":
                case "健康与养生":
                case "家庭与育儿":
                case "学习与发展":
                    object.put("keyword_type_score", roundToThreeDecimals(0.8 * 0.15));
                    break;
            }
            // 判断设备信息
            if (os.equals("Android")){
                object.put("os_score", roundToThreeDecimals(0.4 * 0.1));
            } else if (os.equals("iOS")) {
                object.put("os_score", roundToThreeDecimals(0.3 * 0.1));
            }
        }else if (age != null && age>=50){

            //判断品类
            if (TrendCategoryValues.contains(category1Name)){
                object.put("category_score", roundToThreeDecimals(0.1 * 0.3));
            } else if (HomeCategoryValues.contains(category1Name)) {
                object.put("category_score", roundToThreeDecimals(0.7 * 0.3));
            } else if (HealthCategoryValues.contains(category1Name)) {
                object.put("category_score", roundToThreeDecimals(0.9 * 0.3));
            }
            //判断品牌
            if (EquipmentTmValues.contains(tmName)){
                object.put("tm_score", roundToThreeDecimals(0.1 * 0.2));
            }else if (TrendTmValues.contains(tmName)){
                object.put("tm_score", roundToThreeDecimals(0.9 * 0.2));
            }
            // 判断价格敏感度
            switch (amount) {
                case "高价商品":
                    object.put("price_sensitive_score", roundToThreeDecimals(0.6 * 0.15));
                    break;
                case "中价商品":
                    object.put("price_sensitive_score", roundToThreeDecimals(0.7 * 0.15));
                    break;
                case "低价商品":
                    object.put("price_sensitive_score", roundToThreeDecimals(0.1 * 0.15));
                    break;
            }

            // 判断时间段
            switch (ts) {
                case "凌晨":
                case "夜间":
                    object.put("time_period_score", roundToThreeDecimals(0.1 * 0.1));
                    break;
                case "早晨":
                case "晚上":
                case "中午":
                    object.put("time_period_score", roundToThreeDecimals(0.3 * 0.1));
                    break;
                case "上午":
                case "下午":
                    object.put("time_period_score", roundToThreeDecimals(0.4 * 0.1));
                    break;
            }
            // 判断搜索词
            switch (keywordType) {
                case " 时尚与潮流 ":
                case "科技与数码":
                    object.put("keyword_type_score", roundToThreeDecimals(0.1 * 0.15));
                    break;
                case "性价比":
                    object.put("keyword_type_score", roundToThreeDecimals(0.8 * 0.15));
                    break;
                case "健康与养生":
                    object.put("keyword_type_score", roundToThreeDecimals(0.9 * 0.15));
                    break;
                case "家庭与育儿":
                case "学习与发展":
                    object.put("keyword_type_score", roundToThreeDecimals(0.7 * 0.15));
                    break;
            }
            // 判断设备信息
            if (os.equals("Android")){
                object.put("os_score", roundToThreeDecimals(0.3 * 0.1));
            } else if (os.equals("iOS")) {
                object.put("os_score", roundToThreeDecimals(0.2 * 0.1));
            }
        }


        jsonObject.put("ageScore",object);
        return jsonObject;



    }
}
