package com.cj.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cj.func.TimePeriodFunc;
import com.cj.util.Hbaseutli;
import com.cj.util.flinksink;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

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

        SingleOutputStreamOperator<JSONObject> compareDic = ste.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("category_compare_dic"));


        SingleOutputStreamOperator<JSONObject> orderk = orderInfoFit.keyBy(o -> o.getJSONObject("after").getInteger("id"))
                .intervalJoin(orderDetailFit.keyBy(o -> o.getJSONObject("after").getInteger("order_id")))
                .between(Time.seconds(-60), Time.seconds(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject info, JSONObject detail, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject object = new JSONObject();
                        object.put("id", info.getJSONObject("after").getInteger("id"));
                        object.put("total_amount", info.getJSONObject("after").getDouble("total_amount"));
                        object.put("original_total_amount", info.getJSONObject("after").getDouble("original_total_amount"));
                        object.put("user_id", info.getJSONObject("after").getString("user_id"));
                        object.put("create_time", info.getJSONObject("after").getLong("create_time"));
                        object.put("sku_id", detail.getJSONObject("after").getString("sku_id"));
                        object.put("sku_num", detail.getJSONObject("after").getString("sku_num"));
                        collector.collect(object);
                    }
                });


        SingleOutputStreamOperator<JSONObject> skuK = orderk.map(
                new RichMapFunction<JSONObject, JSONObject>() {


                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = Hbaseutli.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        Hbaseutli.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String skuId = jsonObject.getString("sku_id");
                        JSONObject skuInfoJsonObj = Hbaseutli.getRow(hbaseConn, "gmall_config", "dim_sku_info", skuId, JSONObject.class);
                        JSONObject object = new JSONObject();
                        object.putAll(jsonObject);
                        object.put("sku_name", skuInfoJsonObj.getString("sku_name"));
                        object.put("tm_id", skuInfoJsonObj.getString("tm_id"));
                        object.put("category3_id", skuInfoJsonObj.getString("category3_id"));
                        return object;
                    }

                }
        );
        SingleOutputStreamOperator<JSONObject> b3 = skuK.map(
                new RichMapFunction<JSONObject, JSONObject>() {


                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = Hbaseutli.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        Hbaseutli.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String skuId = jsonObject.getString("category3_id");
                        JSONObject skuInfoJsonObj = Hbaseutli.getRow(hbaseConn, "gmall_config", "dim_base_category3", skuId, JSONObject.class);
                        JSONObject object = new JSONObject();
                        object.putAll(jsonObject);
                        object.put("category3_name", skuInfoJsonObj.getString("name"));

                        object.put("category2_id", skuInfoJsonObj.getString("category2_id"));
                        return object;
                    }

                }
        );
        SingleOutputStreamOperator<JSONObject> b2 = b3.map(
                new RichMapFunction<JSONObject, JSONObject>() {


                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = Hbaseutli.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        Hbaseutli.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String skuId = jsonObject.getString("category2_id");
                        JSONObject skuInfoJsonObj = Hbaseutli.getRow(hbaseConn, "gmall_config", "dim_base_category2", skuId, JSONObject.class);
                        JSONObject object = new JSONObject();
                        object.putAll(jsonObject);
                        object.put("category2_name", skuInfoJsonObj.getString("name"));

                        object.put("category1_id", skuInfoJsonObj.getString("category1_id"));
                        return object;
                    }

                }
        );
        SingleOutputStreamOperator<JSONObject> b1 = b2.map(
                new RichMapFunction<JSONObject, JSONObject>() {


                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = Hbaseutli.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        Hbaseutli.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String skuId = jsonObject.getString("category1_id");
                        JSONObject skuInfoJsonObj = Hbaseutli.getRow(hbaseConn, "gmall_config", "dim_base_category1", skuId, JSONObject.class);
                        JSONObject object = new JSONObject();
                        object.putAll(jsonObject);
                        object.put("category1_name", skuInfoJsonObj.getString("name"));

                        return object;
                    }

                }
        );
        SingleOutputStreamOperator<JSONObject> b = b1.map(
                new RichMapFunction<JSONObject, JSONObject>() {


                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = Hbaseutli.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        Hbaseutli.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String skuId = jsonObject.getString("tm_id");
                        JSONObject skuInfoJsonObj = Hbaseutli.getRow(hbaseConn, "gmall_config", "dim_base_trademark", skuId, JSONObject.class);
                        JSONObject object = new JSONObject();
                        object.putAll(jsonObject);
                        object.put("tm_name", skuInfoJsonObj.getString("tm_name"));

                        return object;
                    }

                }
        );
        SingleOutputStreamOperator<JSONObject> process = b.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject object = new JSONObject();
                object.putAll(jsonObject);
                Double amount = jsonObject.getDouble("total_amount");
                Long aLong = jsonObject.getLong("create_time");
                String ts = TimePeriodFunc.getTimePeriod(aLong);
                String priceRange = TimePeriodFunc.getPriceRange(amount);
                object.put("ts", ts);
                object.put("amount", priceRange);
                collector.collect(object);

            }
        });
//        process.print();
//        process.map(o->JSON.toJSONString(o)).sinkTo(flinksink.getkafkasink("dwd_info"));

        KafkaSource<String> source1 = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("dwd_user_log")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> page = env.fromSource(source1, WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<JSONObject> pageM = page.map(JSON::parseObject);
//        pageM.print();

        SingleOutputStreamOperator<JSONObject> processed = pageM.keyBy(o -> o.getString("uid"))
                .intervalJoin(process.keyBy(o -> o.getString("user_id")))
                .between(Time.seconds(-60), Time.seconds(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        jsonObject.putAll(jsonObject2);
                        jsonObject.remove("uid");
                        collector.collect(jsonObject);



                    }
                });
        processed.print();



        env.execute();
    }
}
