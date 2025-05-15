package com.cj.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cj.func.ProcessJoinBase2And4BaseFunc;
import com.cj.func.ProcessLabelFunc;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Package com.cj.dwd.DbusUserLabel6BaseCalculate
 * @Author chen.jian
 * @Date 2025/5/15 下午6:05
 * @description:
 */
public class DbusUserLabel6BaseCalculate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取kafka中的topic
        KafkaSource<String> source1 = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("kafka_label_base6_topic")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaBase6Source = env.fromSource(source1, WatermarkStrategy.noWatermarks(), "Kafka Source");

        KafkaSource<String> source2 = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("kafka_label_base4_topic")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaBase4Source = env.fromSource(source2, WatermarkStrategy.noWatermarks(), "Kafka Source");


        KafkaSource<String> source3 = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("kafka_label_base2_topic")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaBase2Source = env.fromSource(source3, WatermarkStrategy.noWatermarks(), "Kafka Source");
        //转换成json格式
        SingleOutputStreamOperator<JSONObject> mapBase6 = kafkaBase6Source.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> mapBase4 = kafkaBase4Source.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> mapBase2 = kafkaBase2Source.map(JSON::parseObject);
        //关联mapBase2和mapBase4
        SingleOutputStreamOperator<JSONObject> join2_4Ds = mapBase2.keyBy(o -> o.getString("uid"))
                .intervalJoin(mapBase4.keyBy(o -> o.getString("uid")))
                .between(Time.days(-5), Time.days(5))
                .process(new ProcessJoinBase2And4BaseFunc());
//        join2_4Ds.print();
        //设置乱序水位线
        SingleOutputStreamOperator<JSONObject> waterJoin2_4 = join2_4Ds.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (jsonObject, l) -> jsonObject.getLongValue("ts_ms")));
//        waterJoin2_4.print();

        //进行关联
        SingleOutputStreamOperator<JSONObject> userLabelProcessDs = waterJoin2_4.keyBy(o -> o.getString("uid"))
                .intervalJoin(mapBase6.keyBy(o -> o.getString("uid")))
                .between(Time.days(-5), Time.days(5))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        left.putAll(right);
                        collector.collect(left);
                    }
                });

        userLabelProcessDs.print();
        env.execute ();
    }
}
