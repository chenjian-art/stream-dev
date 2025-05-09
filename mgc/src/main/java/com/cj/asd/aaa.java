package com.cj.asd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.cj.asd.func.*;
import com.cj.asd.utils.CommonGenerateTempLate;
import com.cj.asd.utils.DateTimeUtils;
import com.cj.asd.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.hadoop.hbase.client.Connection;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;

/**
 * @Package com.cj.asd.aaa
 * @Author chen.jian
 * @Date 2025/5/7 18:42
 * @description:
 */
public class aaa {
    private static final ArrayList<String> sensitiveWordsLists;

    static {
        sensitiveWordsLists = SensitiveWordsUtils.getSensitiveWordsLists();
    }
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        "cdh02:9092",
                        "topic_db",
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
//        kafkaCdcDbSource.print();

        DataStream<JSONObject> filteredOrderInfoStream = kafkaCdcDbSource
                .map(JSON::parseObject)
                .filter(json -> json.getJSONObject("source").getString("table").equals("order_info"))
                .uid("kafka_cdc_db_order_source").name("kafka_cdc_db_order_source");
//        filteredOrderInfoStream.print();

        DataStream<JSONObject> filteredStream = kafkaCdcDbSource
                .map(JSON::parseObject)
                .filter(json -> json.getJSONObject("source").getString("table").equals("comment_info"))
                .keyBy(json -> json.getJSONObject("after").getString("appraise"));

//        filteredStream.print();


        SingleOutputStreamOperator<JSONObject> enrichedStream = filteredStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            private Connection hbaseConn;

            @Override
            public void open(Configuration parameters) throws Exception {
                hbaseConn = HBaseUtil.getHBaseConnection();
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeHBaseConnection(hbaseConn);
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {

                String spuId = jsonObject.getJSONObject("after").getString("appraise");
                JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, "gmall_config", "dim_base_dic", spuId, JSONObject.class);
                String dicName = skuInfoJsonObj.getString("dic_name");
                jsonObject.getJSONObject("after").put("dic_name", dicName);


                return jsonObject;
            }
        });

        enrichedStream.print();

        SingleOutputStreamOperator<JSONObject> orderCommentMap = enrichedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject){
                JSONObject resJsonObj = new JSONObject();
                Long tsMs = jsonObject.getLong("ts_ms");
                JSONObject source = jsonObject.getJSONObject("source");
                String dbName = source.getString("db");
                String tableName = source.getString("table");
                String serverId = source.getString("server_id");
                if (jsonObject.containsKey("after")) {
                    JSONObject after = jsonObject.getJSONObject("after");
                    resJsonObj.put("ts_ms", tsMs);
                    resJsonObj.put("db", dbName);
                    resJsonObj.put("table", tableName);
                    resJsonObj.put("server_id", serverId);
                    resJsonObj.put("appraise", after.getString("appraise"));
                    resJsonObj.put("commentTxt", after.getString("comment_txt"));
                    resJsonObj.put("op", jsonObject.getString("op"));
                    resJsonObj.put("nick_name", jsonObject.getString("nick_name"));
                    resJsonObj.put("create_time", after.getLong("create_time"));
                    resJsonObj.put("user_id", after.getLong("user_id"));
                    resJsonObj.put("sku_id", after.getLong("sku_id"));
                    resJsonObj.put("id", after.getLong("id"));
                    resJsonObj.put("spu_id", after.getLong("spu_id"));
                    resJsonObj.put("order_id", after.getLong("order_id"));
                    resJsonObj.put("dic_name", after.getString("dic_name"));
                    return resJsonObj;
                }
                return null;
            }
        });
        orderCommentMap.print();

        SingleOutputStreamOperator<JSONObject> orderInfoMapDs = filteredOrderInfoStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject inputJsonObj){
                String op = inputJsonObj.getString("op");
                long tm_ms = inputJsonObj.getLongValue("ts_ms");
                JSONObject dataObj;
                if (inputJsonObj.containsKey("after") && !inputJsonObj.getJSONObject("after").isEmpty()) {
                    dataObj = inputJsonObj.getJSONObject("after");
                } else {
                    dataObj = inputJsonObj.getJSONObject("before");
                }
                JSONObject resultObj = new JSONObject();
                resultObj.put("op", op);
                resultObj.put("tm_ms", tm_ms);
                resultObj.putAll(dataObj);
                return resultObj;
            }
        }).uid("map-order_info_data").name("map-order_info_data");
        orderInfoMapDs.print();

        KeyedStream<JSONObject, String> keyedOrderCommentStream = orderCommentMap.keyBy(data -> data.getString("order_id"));
        KeyedStream<JSONObject, String> keyedOrderInfoStream = orderInfoMapDs.keyBy(data -> data.getString("id"));


        SingleOutputStreamOperator<JSONObject> orderMsgAllDs = keyedOrderCommentStream.intervalJoin(keyedOrderInfoStream)
                .between(Time.minutes(-1), Time.minutes(1))
                .process(new IntervalJoinOrderCommentAndOrderInfoFunc())
                .uid("interval_join_order_comment_and_order_info_func").name("interval_join_order_comment_and_order_info_func");
        orderMsgAllDs.print();

        SingleOutputStreamOperator<JSONObject> supplementDataMap = orderMsgAllDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                jsonObject.put("commentTxt", CommonGenerateTempLate.GenerateComment(jsonObject.getString("dic_name"), jsonObject.getString("info_trade_body")));
                return jsonObject;
            }
        }).uid("map-generate_comment").name("map-generate_comment");
        supplementDataMap.print();


        SingleOutputStreamOperator<JSONObject> suppleMapDs = supplementDataMap.map(new RichMapFunction<JSONObject, JSONObject>() {
            private transient Random random;

            @Override
            public void open(Configuration parameters){
                random = new Random();
            }

            @Override
            public JSONObject map(JSONObject jsonObject){
                if (random.nextDouble() < 0.2) {
                    jsonObject.put("commentTxt", jsonObject.getString("commentTxt") + "," + SensitiveWordsUtils.getRandomElement(sensitiveWordsLists));
                    System.err.println("change commentTxt: " + jsonObject);
                }
                return jsonObject;
            }
        }).uid("map-sensitive-words").name("map-sensitive-words");
        suppleMapDs.print();


        SingleOutputStreamOperator<JSONObject> suppleTimeFieldDs = suppleMapDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject){
                jsonObject.put("ds", DateTimeUtils.format(new Date(jsonObject.getLong("ts_ms")), "yyyyMMdd"));
                return jsonObject;
            }
        }).uid("add json ds").name("add json ds");
        suppleTimeFieldDs.print();



        suppleTimeFieldDs.map(js -> js.toJSONString())
                .sinkTo(
                        KafkaUtils.buildKafkaSink("cdh02:9092", "fact_comment_topic")
                ).uid("kafka_db_fact_comment_sink").name("kafka_db_fact_comment_sink");




        env.execute();
    }
}
