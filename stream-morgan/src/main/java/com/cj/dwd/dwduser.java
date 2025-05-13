package com.cj.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cj.func.processfilter;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.operators.translation.KeyRemovingMapper;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @Package com.cj.dwd.dwduser
 * @Author chen.jian
 * @Date 2025/5/12 10:21
 * @description:
 */
public class dwduser {
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

                    String zodiacSign = getZodiacSign(date);
                    after.put("zodiac_sign", zodiacSign);
                    int year = date.getYear();
                    int decade = (year / 10) * 10; // 计算年代（如1990, 2000）
                    after.put("decade", decade);

                    LocalDate currentDate = LocalDate.now();
                    int age = calculateAge(date, currentDate);
                    after.put("age", age);

                }
            }
            return json;
        });


        SingleOutputStreamOperator<JSONObject> userK = user.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject object = new JSONObject();
                if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null){
                    JSONObject after = jsonObject.getJSONObject("after");
                    String birthday = after.getString("birthday");

                    String name = after.getString("name");
                    String zodiacSign = after.getString("zodiac_sign");
                    Integer id = after.getInteger("id");
                    Integer birthDecade = after.getInteger("decade");
                    String login_name = after.getString("login_name");
                    String userLevel = after.getString("user_level");
                    String phoneNum = after.getString("phone_num");
                    String email = after.getString("email");
                    Long tsMs = jsonObject.getLong("ts_ms");
                    Integer age = after.getInteger("age");

                    object.put("birthday", birthday);
                    object.put("decade", birthDecade);
                    object.put("name", name);
                    object.put("zodiac_sign", zodiacSign);
                    object.put("id", id);
                    object.put("login_name", login_name);
                    object.put("user_level", userLevel);
                    object.put("phone_num", phoneNum);
                    object.put("gender", after.getString("gender") != null ? after.getString("gender") : "home");
                    object.put("email", email);
                    object.put("ts_ms",tsMs);
                    object.put("age", age);
                }

                return object;
            }
        });


        SingleOutputStreamOperator<JSONObject> sup = ste.map(JSON::parseObject).filter(o -> o.getJSONObject("source").getString("table").equals("user_info_sup_msg"));

        SingleOutputStreamOperator<JSONObject> supK = sup.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                JSONObject object = new JSONObject();
                if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null){
                    JSONObject after = jsonObject.getJSONObject("after");
                    Integer uid = after.getInteger("uid");
                    String height = after.getString("height");
                    String weight = after.getString("weight");
                    String unitWeight = after.getString("unit_weight");
                    String unitHeight = after.getString("unit_height");
                    object.put("uid", uid);
                    object.put("height", height);
                    object.put("weight", weight);
                    object.put("unit_weight", unitWeight);
                    object.put("unit_height", unitHeight);
                }
                return object;
            }
        });



        SingleOutputStreamOperator<JSONObject> ds3 = userK.keyBy(o -> o.getInteger("id"))
                .intervalJoin(supK.keyBy(o -> o.getInteger("uid")))
                .between(Time.seconds(-60), Time.seconds(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject result = new JSONObject();
                        if (jsonObject.getString("id").equals(jsonObject2.getString("uid"))){
                            result.putAll(jsonObject);
                            result.put("height",jsonObject2.getString("height"));
                            result.put("unit_height",jsonObject2.getString("unit_height"));
                            result.put("weight",jsonObject2.getString("weight"));
                            result.put("unit_weight",jsonObject2.getString("unit_weight"));
                        }
                        collector.collect(result);
                    }
                });

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
                JSONObject object = new JSONObject();
                if (jsonObject.containsKey("common")) {
                    JSONObject common = jsonObject.getJSONObject("common");
                    object.put("uid", common.getInteger("uid") != null ? common.getInteger("uid") : "-1");
                    object.put("ts", jsonObject.getLong("ts"));
                    JSONObject object1 = new JSONObject();
                    common.remove("sid");
                    common.remove("is_new");
                    common.remove("mid");
                    object1.putAll(common);
                    object.put("deviceInfo",object1);
                    if (jsonObject.containsKey("page") && !jsonObject.getJSONObject("page").isEmpty()){
                        JSONObject page = jsonObject.getJSONObject("page");
                        if (page.containsKey("item_type") && page.getString("item_type").equals("keyword")){
                            object.put("search_item",page.getString("item"));
                        }
                    }
                }

                JSONObject source2 = object.getJSONObject("deviceInfo");
                String s = source2.getString("os").split(" ")[0];
                source2.put("os",s);
                return object;
            }
        });

        KeyedStream<JSONObject, Integer> pageUid = pagelog.keyBy(o -> o.getInteger("uid"));

        pageUid.process(new processfilter());


        env.execute();
    }

    private static String getZodiacSign(LocalDate date) {
        int month = date.getMonthValue();
        int day = date.getDayOfMonth();

// 定义星座区间映射
        if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) {
            return "摩羯座";
        } else if ((month == 1 && day >= 20) || (month == 2 && day <= 18)) {
            return "水瓶座";
        } else if ((month == 2 && day >= 19) || (month == 3 && day <= 20)) {
            return "双鱼座";
        } else if ((month == 3 && day >= 21) || (month == 4 && day <= 19)) {
            return "白羊座";
        } else if ((month == 4 && day >= 20) || (month == 5 && day <= 20)) {
            return "金牛座";
        } else if ((month == 5 && day >= 21) || (month == 6 && day <= 21)) {
            return "双子座";
        } else if ((month == 6 && day >= 22) || (month == 7 && day <= 22)) {
            return "巨蟹座";
        } else if ((month == 7 && day >= 23) || (month == 8 && day <= 22)) {
            return "狮子座";
        } else if ((month == 8 && day >= 23) || (month == 9 && day <= 22)) {
            return "处女座";
        } else if ((month == 9 && day >= 23) || (month == 10 && day <= 23)) {
            return "天秤座";
        } else if ((month == 10 && day >= 24) || (month == 11 && day <= 22)) {
            return "天蝎座";
        } else if ((month == 11 && day >= 23) || (month == 12 && day <= 21)) {
            return "射手座";
        }
        return "未知"; // 默认情况，实际上不会执行到这一步
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
