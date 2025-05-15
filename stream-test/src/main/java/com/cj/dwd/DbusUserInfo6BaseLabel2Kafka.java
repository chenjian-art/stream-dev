package com.cj.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cj.base.DimBaseCategory;
import com.cj.func.*;
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
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.paimon.flink.sink.FlinkSink;

import java.sql.Connection;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * @Package com.cj.dwd.DbusUserInfo6BaseLabel2Kafka
 * @Author chen.jian
 * @Date 2025/5/15 下午4:04
 * @description:
 */
public class DbusUserInfo6BaseLabel2Kafka {
    private static final List<DimBaseCategory> dim_base_categories;
    private static final Connection connection;

    private static final double device_rate_weight_coefficient = 0.1; // 设备权重系数
    private static final double search_rate_weight_coefficient = 0.15; // 搜索权重系数
    private static final double time_rate_weight_coefficient = 0.1;    // 时间权重系数
    private static final double amount_rate_weight_coefficient = 0.15;    // 价格权重系数
    private static final double brand_rate_weight_coefficient = 0.2;    // 品牌权重系数
    private static final double category_rate_weight_coefficient = 0.3; // 类目权重系数

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
//        获取kafka主题
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("topic_db")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaDb = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");


        KafkaSource<String> source1 = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("dwd_page")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaPage = env.fromSource(source1, WatermarkStrategy.noWatermarks(), "Kafka Source");
        //将数据源转换成JSON对象
        SingleOutputStreamOperator<JSONObject> dataJsonDs = kafkaDb.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> dataPageDs = kafkaPage.map(JSON::parseObject);
        //提取出需要的字段信息
        SingleOutputStreamOperator<JSONObject> logDs = dataPageDs.map(new RichMapFunction<JSONObject, JSONObject>() {
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
        //过滤出uid为空的数据
        SingleOutputStreamOperator<JSONObject> filterDs = logDs.filter(data -> !data.getString("uid").isEmpty());
        //分组
        KeyedStream<JSONObject, String> keyDs = filterDs.keyBy(data -> data.getString("uid"));
        //去重
        SingleOutputStreamOperator<JSONObject> processDs = keyDs.process(new ProcessFilterRepeatTsDataFunc());
        //开窗口
        SingleOutputStreamOperator<JSONObject> winDs = processDs.keyBy(data -> data.getString("uid"))
                .process(new AggregateUserDataProcessFunction())
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2);
        //打分计算
        SingleOutputStreamOperator<JSONObject> mapDs = winDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories, device_rate_weight_coefficient, search_rate_weight_coefficient));
        //过滤出user_info
        SingleOutputStreamOperator<JSONObject> infoDs = dataJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("user_info"));
        //过滤出order_info
        SingleOutputStreamOperator<JSONObject> OrderDs = dataJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("order_info"));
        //过滤出order_detail
        SingleOutputStreamOperator<JSONObject> detailDs = dataJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("order_detail"));
        //将order_info和order_detail进行转换类型
        SingleOutputStreamOperator<JSONObject> mapCdcOrderInfoDs = OrderDs.map(new MapOrderInfoDataFunc());
        SingleOutputStreamOperator<JSONObject> mapCdcOrderDetailDs = detailDs.map(new MapOrderDetailFunc());
        //过滤出id为空的数据
        SingleOutputStreamOperator<JSONObject> filterNotNullDs = mapCdcOrderInfoDs.filter(data -> data.getString("id") != null && !data.getString("id").isEmpty());
        SingleOutputStreamOperator<JSONObject> filterNotNull = mapCdcOrderDetailDs.filter(data -> data.getString("order_id") != null && !data.getString("order_id").isEmpty());
        //分组
        KeyedStream<JSONObject, String> keyedStreamDs = filterNotNullDs.keyBy(data -> data.getString("id"));
        KeyedStream<JSONObject, String> OrderDetailDs = filterNotNull.keyBy(data -> data.getString("order_id"));
        //对两个流进行合并提取出需要的字段
        SingleOutputStreamOperator<JSONObject> process = keyedStreamDs.intervalJoin(OrderDetailDs)
                .between(Time.minutes(-2), Time.minutes(2))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject result = new JSONObject();
                        result.putAll(jsonObject1);
                        result.put("sku_num",jsonObject2.getLongValue("sku_num"));
                        result.put("split_coupon_amount",jsonObject2.getString("sku_num"));
                        result.put("sku_name",jsonObject2.getString("sku_name"));
                        result.put("order_price",jsonObject2.getString("order_price"));
                        result.put("detail_id",jsonObject2.getString("id"));
                        result.put("order_id",jsonObject2.getString("order_id"));
                        result.put("sku_id",jsonObject2.getLongValue("sku_id"));
                        result.put("split_activity_amount",jsonObject2.getLongValue("split_activity_amount"));
                        result.put("split_total_amount",jsonObject2.getLongValue("split_total_amount"));
                        collector.collect(result);
                    }
                });
        //先对detail_id进行分组后在对数据进行去重 根据时间来去重 时间戳大于存储的时间戳
        SingleOutputStreamOperator<JSONObject> processInfo = process.keyBy(data -> data.getString("detail_id"))
                .process(new processOrderInfoAndDetailFunc());

        // 品类 品牌 年龄 时间 base4
        SingleOutputStreamOperator<JSONObject> mapModelDs = processInfo.map(new MapOrderAndDetailRateModelFunc(dim_base_categories, time_rate_weight_coefficient, amount_rate_weight_coefficient, brand_rate_weight_coefficient, category_rate_weight_coefficient));

        //将读取出来的birthday转换成年月日的形式
        SingleOutputStreamOperator<JSONObject> finalUserInfoDs = infoDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject){
                JSONObject after = jsonObject.getJSONObject("after");
                if (after != null && after.containsKey("birthday")) {
                    Integer epochDay = after.getInteger("birthday");
                    if (epochDay != null) {
                        LocalDate date = LocalDate.ofEpochDay(epochDay);
                        after.put("birthday", date.format(DateTimeFormatter.ISO_DATE));
                    }
                }
                return jsonObject;
            }
        });
        //过滤出user_info_sup_msg
        SingleOutputStreamOperator<JSONObject> userDs = mapDs.filter(data -> data.getJSONObject("source").getString("table").equals("user_info_sup_msg"));
        //提取字段并添加 星座，年龄，性别
        SingleOutputStreamOperator<JSONObject> mapInfoDs = finalUserInfoDs.map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject){
                        JSONObject result = new JSONObject();
                        if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            result.put("uid", after.getString("id"));
                            result.put("uname", after.getString("name"));
                            result.put("user_level", after.getString("user_level"));
                            result.put("login_name", after.getString("login_name"));
                            result.put("phone_num", after.getString("phone_num"));
                            result.put("email", after.getString("email"));
                            result.put("gender", after.getString("gender") != null ? after.getString("gender") : "home");
                            result.put("birthday", after.getString("birthday"));
                            result.put("ts_ms", jsonObject.getLongValue("ts_ms"));
                            String birthdayStr = after.getString("birthday");
                            if (birthdayStr != null && !birthdayStr.isEmpty()) {
                                try {
                                    LocalDate birthday = LocalDate.parse(birthdayStr, DateTimeFormatter.ISO_DATE);
                                    LocalDate currentDate = LocalDate.now(ZoneId.of("Asia/Shanghai"));
                                    int age = calculateAge(birthday, currentDate);
                                    int decade = birthday.getYear() / 10 * 10;
                                    result.put("decade", decade);
                                    result.put("age", age);
                                    String zodiac = getZodiacSign(birthday);
                                    result.put("zodiac_sign", zodiac);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }

                        return result;
                    }
                });
        //提取数据
        SingleOutputStreamOperator<JSONObject> InfoSupDs = userDs.map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) {
                        JSONObject result = new JSONObject();
                        if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            result.put("uid", after.getString("uid"));
                            result.put("unit_height", after.getString("unit_height"));
                            result.put("create_ts", after.getLong("create_ts"));
                            result.put("weight", after.getString("weight"));
                            result.put("unit_weight", after.getString("unit_weight"));
                            result.put("height", after.getString("height"));
                            result.put("ts_ms", jsonObject.getLong("ts_ms"));
                        }
                        return result;
                    }
                });
        //过滤两个流uid为空的数据
        SingleOutputStreamOperator<JSONObject> finalUserinfoDs = mapInfoDs.filter(data -> data.containsKey("uid") && !data.getString("uid").isEmpty());
        SingleOutputStreamOperator<JSONObject> finalUserinfoSupDs = InfoSupDs.filter(data -> data.containsKey("uid") && !data.getString("uid").isEmpty());
        //对uid进行分组
        KeyedStream<JSONObject, String> keyedStreamUserInfoDs = finalUserinfoDs.keyBy(data -> data.getString("uid"));
        KeyedStream<JSONObject, String> keyedStreamUserInfoSupDs = finalUserinfoSupDs.keyBy(data -> data.getString("uid"));
        //对两个流进行关联
        SingleOutputStreamOperator<JSONObject> processJoinDs = keyedStreamUserInfoDs.intervalJoin(keyedStreamUserInfoSupDs)
                .between(Time.minutes(-5), Time.minutes(5))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject result = new JSONObject();
                        if (jsonObject1.getString("uid").equals(jsonObject2.getString("uid"))){
                            result.putAll(jsonObject1);
                            result.put("height",jsonObject2.getString("height"));
                            result.put("unit_height",jsonObject2.getString("unit_height"));
                            result.put("weight",jsonObject2.getString("weight"));
                            result.put("unit_weight",jsonObject2.getString("unit_weight"));
                        }
                        collector.collect(result);
                    }
                });


//        将数据打印并输出到kafka
//        processJoinDs.map(data -> data.toJSONString())
//                .sinkTo(
//                        flinksink.getkafkasink("kafka_label_base6_topic")
//                );
//        processJoinDs.print("JoinDs--->");
//        mapModelDs.map(data -> data.toJSONString())
//                .sinkTo(
//                        flinksink.getkafkasink("kafka_label_base4_topic")
//                );
//        mapModelDs.print("Mode--->");
//        mapDs.map(data -> data.toJSONString())
//                .sinkTo(
//                        flinksink.getkafkasink("kafka_label_base2_topic")
//                );
//        mapDs.print("map--->");

        env.execute();
    }

    private static int calculateAge(LocalDate birthDate, LocalDate currentDate) {
        return Period.between(birthDate, currentDate).getYears();
    }

    private static String getZodiacSign(LocalDate birthDate) {
        int month = birthDate.getMonthValue();
        int day = birthDate.getDayOfMonth();

        // 星座日期范围定义
        if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) return "摩羯座";
        else if (month == 1 || month == 2 && day <= 18) return "水瓶座";
        else if (month == 2 || month == 3 && day <= 20) return "双鱼座";
        else if (month == 3 || month == 4 && day <= 19) return "白羊座";
        else if (month == 4 || month == 5 && day <= 20) return "金牛座";
        else if (month == 5 || month == 6 && day <= 21) return "双子座";
        else if (month == 6 || month == 7 && day <= 22) return "巨蟹座";
        else if (month == 7 || month == 8 && day <= 22) return "狮子座";
        else if (month == 8 || month == 9 && day <= 22) return "处女座";
        else if (month == 9 || month == 10 && day <= 23) return "天秤座";
        else if (month == 10 || month == 11 && day <= 22) return "天蝎座";
        else return "射手座";
    }

}
