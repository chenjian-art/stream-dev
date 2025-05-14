package com.cj.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cj.asd.func.ConfigUtils;
import com.cj.bean.DimBaseCategory;
import com.cj.func.MapDeviceAndSearchMarkModelFunc;
import com.cj.func.ProcessFilterRepeatTsDataFunc;
import com.cj.func.processfilter;
import com.cj.util.JdbcUtils;
import com.cj.util.flinksink;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * @Package com.cj.dwd.dwduser
 * @Author chen.jian
 * @Date 2025/5/12 10:21
 * @description:
 */
public class dwduser {
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

        SingleOutputStreamOperator<JSONObject> stre = ste.map(JSON::parseObject)
                .filter(o -> o.getJSONObject("source").getString("table").equals("user_info"));

        SingleOutputStreamOperator<JSONObject> user = stre.map(jsonStr -> {
            JSONObject json = JSON.parseObject(String.valueOf(jsonStr));
            JSONObject after = json.getJSONObject("after");
            if (after != null && after.containsKey("birthday")) {
                Integer epochDay = after.getInteger("birthday");
                if (epochDay != null) {
                    LocalDate date = LocalDate.ofEpochDay(epochDay);
                    after.put("birthday", date.format(DateTimeFormatter.ISO_DATE));
                }
            }
            return json;
        });


        SingleOutputStreamOperator<JSONObject> userK = user.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
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


        SingleOutputStreamOperator<JSONObject> sup = ste.map(JSON::parseObject).filter(o -> o.getJSONObject("source").getString("table").equals("user_info_sup_msg"));

        SingleOutputStreamOperator<JSONObject> supK = sup.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
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

        SingleOutputStreamOperator<JSONObject> finalUserinfoDs = userK.filter(data -> data.containsKey("uid") && !data.getString("uid").isEmpty());
        SingleOutputStreamOperator<JSONObject> finalUserinfoSupDs = supK.filter(data -> data.containsKey("uid") && !data.getString("uid").isEmpty());

        KeyedStream<JSONObject, String> keyedStreamUserInfoDs = finalUserinfoDs.keyBy(data -> data.getString("uid"));
        KeyedStream<JSONObject, String> keyedStreamUserInfoSupDs = finalUserinfoSupDs.keyBy(data -> data.getString("uid"));


        SingleOutputStreamOperator<JSONObject> ds3 = keyedStreamUserInfoDs.intervalJoin(keyedStreamUserInfoSupDs)
                .between(Time.minutes(-5), Time.minutes(5))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject result = new JSONObject();
                        if (jsonObject1.getString("uid").equals(jsonObject2.getString("uid"))) {
                            result.putAll(jsonObject1);
                            result.put("height", jsonObject2.getString("height"));
                            result.put("unit_height", jsonObject2.getString("unit_height"));
                            result.put("weight", jsonObject2.getString("weight"));
                            result.put("unit_weight", jsonObject2.getString("unit_weight"));
                        }
                        collector.collect(result);
                    }
                });

        ds3.print();
//        ds3.map(o -> JSON.toJSONString(o)).sinkTo(flinksink.getkafkasink("dwd_user_log"));


//      page日志信息
        KafkaSource<String> source1 = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("dwd_page")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkalog = env.fromSource(source1, WatermarkStrategy.noWatermarks(), "Kafka Source");

        SingleOutputStreamOperator<JSONObject> logJson = kafkalog.map(JSON::parseObject);

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
        SingleOutputStreamOperator<JSONObject> filtered = pagelog.filter(o -> !o.getString("uid").isEmpty());
//        filtered.print();
        KeyedStream<JSONObject, String> keyedSteamLogPage = filtered.keyBy(o -> o.getString("uid"));
//        keyedSteamLogPage.print();
        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedSteamLogPage.process(new ProcessFilterRepeatTsDataFunc());
//        processStagePageLogDs.print();

        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(o -> o.getString("uid")).
                process(new processfilter())
                .keyBy(o -> o.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2);
//        win2MinutesPageLogsDs.print();
        SingleOutputStreamOperator<JSONObject> susdf = win2MinutesPageLogsDs.map(new MapDeviceAndSearchMarkModelFunc(dim_base_categories, device_rate_weight_coefficient, search_rate_weight_coefficient));
//        susdf.print();
//        susdf.map(o-> JSON.toJSONString(o)).sinkTo(flinksink.getkafkasink("dwd_page_info"));

        env.execute();
    }

    private static String getZodiacSign(LocalDate date) {
        int month = date.getMonthValue();
        int day = date.getDayOfMonth();

// 定义星座区间映射
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
    private static int calculateAge(LocalDate birthDate, LocalDate currentDate) {
// 如果生日日期晚于当前日期，抛出异常
        if (birthDate.isAfter(currentDate)) {
            throw new IllegalArgumentException("生日日期不能晚于当前日期");
        }

        int age = currentDate.getYear() - birthDate.getYear();

// 如果当前月份小于生日月份，或者月份相同但日期小于生日日期，则年龄减1
        if (currentDate.getMonthValue() < birthDate.getMonthValue() ||
                (currentDate.getMonthValue() == birthDate.getMonthValue() &&
                        currentDate.getDayOfMonth() < birthDate.getDayOfMonth())) {
            age--;
        }

        return age;
    }

}
