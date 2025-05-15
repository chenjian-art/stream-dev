package com.cj.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cj.func.JudgmentFunc;
import com.cj.func.PriceTime;
import com.cj.func.pricebase;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.paimon.flink.procedure.ProcedureBase;

import java.rmi.server.UID;

/**
 * @Package com.cj.dwd.dwdjuhe
 * @Author chen.jian
 * @Date 2025/5/14 下午3:23
 * @description:
 */
public class dwdjuhe {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        获取kafka数据
        KafkaSource<String> source1 = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("dwd_info")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> info = env.fromSource(source1, WatermarkStrategy.noWatermarks(), "Kafka Source");

        KafkaSource<String> source2 = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("dwd_user_log")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> user = env.fromSource(source2, WatermarkStrategy.noWatermarks(), "Kafka Source");


//        转成json
        SingleOutputStreamOperator<JSONObject> infoM = info.map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> userM = user.map(JSON::parseObject);

//        关联
        SingleOutputStreamOperator<JSONObject> userinfo = infoM.keyBy(o -> o.getString("user_id"))
                .intervalJoin(userM.keyBy(o -> o.getString("uid")))
                .between(Time.seconds(-60), Time.seconds(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        jsonObject.putAll(jsonObject2);
                        jsonObject.remove("uid");
                        collector.collect(jsonObject);
                    }
                });




        KafkaSource<String> source3 = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("dwd_page_info")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> order = env.fromSource(source3, WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<JSONObject> map = order.map(JSON::parseObject);
//        和page进行关联
        SingleOutputStreamOperator<JSONObject> processed = map.keyBy(o -> o.getString("uid"))
                .intervalJoin(userinfo.keyBy(o -> o.getString("user_id")))
                .between(Time.days(-5), Time.days(5))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        jsonObject.putAll(jsonObject2);
                        jsonObject.remove("uid");
                        collector.collect(jsonObject);
                    }
                });
//        processed.print();


        //读取kafka
        KafkaSource<String> source4 = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("topic_db")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> orderDic = env.fromSource(source4, WatermarkStrategy.noWatermarks(), "Kafka Source");
        //转换成json并过滤表
        SingleOutputStreamOperator<JSONObject> filter = orderDic.map(JSON::parseObject).filter(o -> o.getJSONObject("source").getString("table").equals("category_compare_dic"));

        //关联两表
        SingleOutputStreamOperator<JSONObject> tringf = processed.keyBy(o -> o.getString("search_item"))
                .intervalJoin(filter.keyBy(o -> o.getJSONObject("after").getString("category_name")))
                .between(Time.days(-5), Time.days(5))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {

                        JSONObject after = jsonObject2.getJSONObject("after");
                        jsonObject.putAll(after);
                        collector.collect(jsonObject);
                    }
                });
//        tringf.print();
        //对值进行计算
        SingleOutputStreamOperator<JSONObject> mapped = tringf.map(new pricebase());
        //输出结果
//        mapped.print();

        //判断年龄的区间
        SingleOutputStreamOperator<JSONObject> operator = mapped.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                jsonObject.put("age_qj", JudgmentFunc.ageJudgment(jsonObject.getInteger("age")));
                collector.collect(jsonObject);
            }
        });
        operator.print();

        env.execute();
    }
}
