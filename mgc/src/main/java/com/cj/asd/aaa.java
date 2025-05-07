package com.cj.asd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.cj.asd.func.AsyncHbaseDimBaseDicFunc;
import com.cj.asd.func.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;

import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @Package com.cj.asd.aaa
 * @Author chen.jian
 * @Date 2025/5/7 18:42
 * @description:
 */
public class aaa {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String kafka_botstrap_servers = "cdh02:9092";
        String kafka_cdc_db_topic = "topic_db";
        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        kafka_botstrap_servers,
                        kafka_cdc_db_topic,
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                                    if (event != null){
                                        try {
                                            return JSONObject.parseObject(event).getLong("ts_ms");
                                        }catch (Exception e){
                                            e.printStackTrace();
                                            System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                            return 0L;
                                        }
                                    }
                                    return 0L;
                                }
                        ),
                "kafka_cdc_db_source"
        ).uid("kafka_cdc_db_source").name("kafka_cdc_db_source");


        DataStream<JSONObject> filteredOrderInfoStream = kafkaCdcDbSource
                .map(JSON::parseObject)
                .filter(json -> json.getJSONObject("source").getString("table")
                        .equals("order_info"));

        DataStream<JSONObject> filteredStream = kafkaCdcDbSource
                .map(JSON::parseObject)
                .filter(json -> {
                    JSONObject sourceObj = json.getJSONObject("source");
                    return sourceObj != null && "comment_info".equals(sourceObj.getString("table"));
                })
                .filter(json -> {
                    JSONObject afterObj = json.getJSONObject("after");
                    return afterObj != null && afterObj.getString("appraise") != null;
                })
                .keyBy(json -> json.getJSONObject("after").getString("appraise"));



        DataStream<JSONObject> enrichedStream = AsyncDataStream.unorderedWait(
                filteredStream,
                new AsyncHbaseDimBaseDicFunc(),
                60,
                TimeUnit.SECONDS,
                100
        );
        enrichedStream.print();

//        SingleOutputStreamOperator<JSONObject> orderCommentMap = enrichedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
//            @Override
//            public JSONObject map(JSONObject jsonObject){
//                JSONObject resJsonObj = new JSONObject();
//                Long tsMs = jsonObject.getLong("ts_ms");
//                JSONObject source = jsonObject.getJSONObject("source");
//                String dbName = source.getString("db");
//                String tableName = source.getString("table");
//                String serverId = source.getString("server_id");
//                if (jsonObject.containsKey("after")) {
//                    JSONObject after = jsonObject.getJSONObject("after");
//                    resJsonObj.put("ts_ms", tsMs);
//                    resJsonObj.put("db", dbName);
//                    resJsonObj.put("table", tableName);
//                    resJsonObj.put("server_id", serverId);
//                    resJsonObj.put("appraise", after.getString("appraise"));
//                    resJsonObj.put("commentTxt", after.getString("comment_txt"));
//                    resJsonObj.put("op", jsonObject.getString("op"));
//                    resJsonObj.put("nick_name", jsonObject.getString("nick_name"));
//                    resJsonObj.put("create_time", after.getLong("create_time"));
//                    resJsonObj.put("user_id", after.getLong("user_id"));
//                    resJsonObj.put("sku_id", after.getLong("sku_id"));
//                    resJsonObj.put("id", after.getLong("id"));
//                    resJsonObj.put("spu_id", after.getLong("spu_id"));
//                    resJsonObj.put("order_id", after.getLong("order_id"));
//                    resJsonObj.put("dic_name", after.getString("dic_name"));
//                    return resJsonObj;
//                }
//                return null;
//            }
//        });
//
//        orderCommentMap.print();


//        SingleOutputStreamOperator<JSONObject> orderInfoMapDs = filteredOrderInfoStream.map(new RichMapFunction<JSONObject, JSONObject>() {
//            @Override
//            public JSONObject map(JSONObject inputJsonObj){
//                String op = inputJsonObj.getString("op");
//                long tm_ms = inputJsonObj.getLongValue("ts_ms");
//                JSONObject dataObj;
//                if (inputJsonObj.containsKey("after") && !inputJsonObj.getJSONObject("after").isEmpty()) {
//
//                    dataObj = inputJsonObj.getJSONObject("after");
//
//                } else {
//                    dataObj = inputJsonObj.getJSONObject("before");
//                }
//                JSONObject resultObj = new JSONObject();
//                resultObj.put("op", op);
//                resultObj.put("tm_ms", tm_ms);
//                resultObj.putAll(dataObj);
//                return resultObj;
//            }
//        });
//        orderInfoMapDs.print();






        env.execute();
    }
}
