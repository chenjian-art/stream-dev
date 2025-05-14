package com.cj.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
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



        SingleOutputStreamOperator<JSONObject> infoM = info.map(JSON::parseObject);
//        "amount":"高价商品","category2_name":"大 家 电","create_time":1747092434000,"sku_num":"1","sku_id":"35","category1_id":"3","tm_name":"联想","tm_id":"3","total_amount":5499.0,"user_id":"1","category1_name":"家用电器","sku_name":"华为智慧屏V65i 65英寸 HEGE-560B 4K全面屏智能电视机 多方视频通话 AI升降摄像头 4GB+32GB 星际黑","id":9685,"category3_name":"平板电视","category3_id":"86","category2_id":"16","ts":"早晨"}
//        infoM.print("info--->");
        SingleOutputStreamOperator<JSONObject> userM = user.map(JSON::parseObject);
//        {"birthday":"1973-01-12","decade":1970,"uname":"慕容发武","gender":"home","zodiac_sign":"摩羯座","weight":"78","uid":"1","login_name":"tmnoibvwno3","unit_height":"cm","user_level":"1","phone_num":"13613344117","unit_weight":"kg","email":"tmnoibvwno3@gmail.com","ts_ms":1747101070095,"age":52,"height":"164"}        userM.print("user--->");
//        userM.print("user--->");



        infoM.keyBy(o->o.getString("user_id"))
            .intervalJoin(userM.keyBy(o->o.getString("uid")))
                .between(Time.seconds(-60),Time.seconds(60))
                    .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                        @Override
                        public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                            jsonObject.putAll(jsonObject2);
                            jsonObject.remove("uid");
                            collector.collect(jsonObject);
                        }
                    }).print("user--->");


        env.execute();
    }
}
