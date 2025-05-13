package com.cj.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Package com.cj.dwd.dwdcategory
 * @Author chen.jian
 * @Date 2025/5/13 10:31
 * @description:
 */
public class dwdcategory {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("topic_db")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> ste = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        SingleOutputStreamOperator<JSONObject> orderInfoFit = ste.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("order_info"));

        SingleOutputStreamOperator<JSONObject> orderDetailFit = ste.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("order_detail"));

        SingleOutputStreamOperator<JSONObject> skuInfoFit = ste.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("sku_info"));

        SingleOutputStreamOperator<JSONObject> baseFit = ste.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("base_trademark"));



        SingleOutputStreamOperator<JSONObject> process = orderInfoFit.keyBy(o -> o.getJSONObject("after").getInteger("id"))
                .intervalJoin(orderDetailFit.keyBy(o -> o.getJSONObject("after").getInteger("order_id")))
                .between(Time.seconds(-60), Time.seconds(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject info, JSONObject detail, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject object = new JSONObject();
                        object.put("order_info", info.getJSONObject("after"));

                        collector.collect(object);
                    }
                });


        process.print();

        env.execute();
    }
}
