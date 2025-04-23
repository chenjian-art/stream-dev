package com.example.tealtimespring.controller;

import com.example.tealtimespring.bean.TradeProvinceOrderAmount;
import com.example.tealtimespring.service.TradeStatsService;
import com.example.tealtimespring.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Package com.example.tealtimespring.controller.TradeStatsController
 * @Author chen.jian
 * @Date 2025/4/22 16:57
 * @description: 交易
 */
@RestController
public class TradeStatsController {
    @Autowired
    private TradeStatsService tradeStatsService;

    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if(date == 0){
            date = DateFormatUtil.now();
        }
        BigDecimal gmv = tradeStatsService.getGMV(date);
        String json = "{\n" +
                "\t\t  \"status\": 0,\n" +
                "\t\t  \"data\": "+gmv+"\n" +
                "\t\t}";
        return json;
    }

    @RequestMapping("/province")
    public String getProvinceAmount(@RequestParam(value = "date",defaultValue = "0") Integer date) {
        if (date == 0) {
            date = DateFormatUtil.now();
        }
        List<TradeProvinceOrderAmount> provinceOrderAmountList = tradeStatsService.getProvinceAmount(date);

        StringBuilder jsonB = new StringBuilder("{\"status\": 0,\"data\": {\"mapData\": [");
        for (int i = 0; i < provinceOrderAmountList.size(); i++) {
            TradeProvinceOrderAmount provinceOrderAmount = provinceOrderAmountList.get(i);
            jsonB.append("{\"name\": \"" + provinceOrderAmount.getProvinceName() + "\",\"value\": " + provinceOrderAmount.getOrderAmount() + "}");
            if (i < provinceOrderAmountList.size() - 1) {
                jsonB.append(",");
            }
        }

        jsonB.append("],\"valueName\": \"交易额\"}}");
        return jsonB.toString();
    }
}
