package com.cj.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cj.bean.DimBaseCategory;
import com.cj.func.MapDeviceAndSearchMarkModelFunc;
import com.cj.func.ProcessFilterRepeatTsDataFunc;
import com.cj.func.processfilter;
import com.cj.util.JdbcUtils;
import com.cj.util.flinksink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Connection;
import java.util.List;

/**
 * @Package com.cj.dwd.dwdpage
 * @Author chen.jian
 * @Date 2025/5/15 下午1:51
 * @description:
 */
public class dwdpage {
    private static final List<DimBaseCategory> dim_base_categories;
    private static final Connection connection;
    private static final double device_rate_weight_coefficient = 0.1;
    private static final double search_rate_weight_coefficient = 0.15;

    static {
        try {
            connection = JdbcUtils.getMySQLConnection(
                    "jdbc:mysql://cdh03:3306/gmall_config?useSSL=false",
                    "root",
                    "root");
            String sql = "select b3.id,                          \n" +
                    "            b3.name as b3name,              \n" +
                    "            b2.name as b2name,              \n" +
                    "            b1.name as b1name               \n" +
                    "     from gmall_config.base_category3 as b3  \n" +
                    "     join gmall_config.base_category2 as b2  \n" +
                    "     on b3.category2_id = b2.id             \n" +
                    "     join gmall_config.base_category1 as b1  \n" +
                    "     on b2.category1_id = b1.id";
            dim_base_categories = JdbcUtils.queryList2(connection, sql, DimBaseCategory.class, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        page日志信息
        KafkaSource<String> source1 = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("dwd_page")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkalog = env.fromSource(source1, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        转成json
        SingleOutputStreamOperator<JSONObject> logJson = kafkalog.map(JSON::parseObject);
//        logJson.print();

//        获取字段信息
        SingleOutputStreamOperator<JSONObject> pagelog = logJson.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject result = new JSONObject();
                if (jsonObject.containsKey("common")){
                    JSONObject common = jsonObject.getJSONObject("common");
                    result.put("uid",common.getString("uid") != null ? common.getString("uid") : "-1");
                    result.put("ts",jsonObject.getLongValue("ts"));
                    JSONObject deviceInfo = new JSONObject();
                    common.remove("sid");
                    common.remove("mid");
                    common.remove("is_new");
                    deviceInfo.putAll(common);
                    result.put("deviceInfo",deviceInfo);
                    if(jsonObject.containsKey("page") && !jsonObject.getJSONObject("page").isEmpty()){
                        JSONObject pageInfo = jsonObject.getJSONObject("page");
                        if (pageInfo.containsKey("item_type") && pageInfo.getString("item_type").equals("keyword")){
                            String item = pageInfo.getString("item");
                            result.put("search_item",item);
                        }
                    }
                }
                JSONObject deviceInfo = result.getJSONObject("deviceInfo");
                String os = deviceInfo.getString("os").split(" ")[0];
                deviceInfo.put("os",os);
                return result;
            }
        });
//        pagelog.print();
//        判断不为空
        SingleOutputStreamOperator<JSONObject> filtered = pagelog.filter(o -> !o.getString("uid").isEmpty());
//        filtered.print();
//        分组
        KeyedStream<JSONObject, String> keyedSteamLogPage = filtered.keyBy(o -> o.getString("uid"));
//        keyedSteamLogPage.print();
//        去重
        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedSteamLogPage.process(new ProcessFilterRepeatTsDataFunc());
//        processStagePageLogDs.print();
//        2minutes窗口
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(o -> o.getString("uid")).
                process(new processfilter())
                .keyBy(o -> o.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2);
        win2MinutesPageLogsDs.print();
        win2MinutesPageLogsDs.map(o-> JSON.toJSONString(o)).sinkTo(flinksink.getkafkasink("dwd_v2_page_info"));
//        打分
        SingleOutputStreamOperator<JSONObject> susdf = win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories, device_rate_weight_coefficient, search_rate_weight_coefficient));
//        susdf.print();
//        susdf.map(o-> JSON.toJSONString(o)).sinkTo(flinksink.getkafkasink("dwd_page_info"));

        env.execute();
    }
}
