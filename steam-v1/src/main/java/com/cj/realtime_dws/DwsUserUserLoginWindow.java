package com.cj.realtime_dws;

import com.cj.bean.UserLoginBean;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import com.cj.utils.dataformtutil;
import com.cj.utils.finksink;

/**
 * @Package com.cj.realtime_dws.DwsUserUserLoginWindow
 * @Author chen.jian
 * @Date 2025/4/15 10:52
 * @description: 独立用户以及回流用户聚合统计
 */
//
public class DwsUserUserLoginWindow {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        设置了检查点的超时时间为 60000 毫秒（即 60 秒）。如果在 60 秒内检查点操作没有完成，就会被视为失败。
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        当作业被取消时，检查点数据不会被删除，而是会保留下来，这样在后续需要时可以利用这些检查点数据进行恢复操作。
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        两次检查点操作之间的最小间隔时间为 2000 毫秒（即 2 秒）。这是为了避免在短时间内频繁进行检查点操作，从而影响作业的正常处理性能。
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        表示在 30 天内允许的最大失败次数为 3 次。
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.days(30), org.apache.flink.api.common.time.Time.seconds(3)));
//        状态后端用于管理 Flink 作业的状态数据，HashMapStateBackend 会将状态数据存储在 TaskManager 的内存中，适用于小规模的状态管理。
        env.setStateBackend(new HashMapStateBackend());
//        数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("dwd_traffic_page")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        //转换类型
        SingleOutputStreamOperator<JSONObject> map = kafkaStrDS.map(JSON::parseObject);
//        map.print();
//        2> {"common":{"ar":"25","uid":"382","os":"iOS 13.2.9","ch":"Appstore","is_new":"1","md":"iPhone 14 Plus","mid":"mid_50","vc":"v2.1.134","ba":"iPhone","sid":"dde82200-6622-47fd-a2f3-08489fcb2a95"},"page":{"page_id":"payment","item":"3742","during_time":17322,"item_type":"order_id","last_page_id":"order"},"ts":1744816572878}

        //过滤uid
        SingleOutputStreamOperator<JSONObject> filter = map.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject)   {
                String uid = jsonObject.getJSONObject("common").getString("uid");
                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                return StringUtils.isNotEmpty(uid)
                        && ("login".equals(lastPageId) || StringUtils.isEmpty(lastPageId));
            }
        });
//        filter.print();
//        2> {"common":{"ar":"32","uid":"456","os":"iOS 13.3.1","ch":"Appstore","is_new":"0","md":"iPhone 13","mid":"mid_68","vc":"v2.1.134","ba":"iPhone","sid":"91bd8ae7-9200-41e9-8df6-3903e9e8a322"},"page":{"page_id":"mine","during_time":10319,"last_page_id":"payment"},"ts":1744816393468}


//        ts.print();
//        2> {"common":{"ar":"32","uid":"456","os":"iOS 13.3.1","ch":"Appstore","is_new":"0","md":"iPhone 13","mid":"mid_68","vc":"v2.1.134","ba":"iPhone","sid":"91bd8ae7-9200-41e9-8df6-3903e9e8a322"},"page":{"page_id":"payment","item":"3746","during_time":14290,"item_type":"order_id","last_page_id":"order"},"ts":1744816373149}
        //分组
        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = filter.keyBy(o -> o.getJSONObject("common").getString("uid"));

        //状态编程
        SingleOutputStreamOperator<UserLoginBean> process = jsonObjectStringKeyedStream.process(new ProcessFunction<JSONObject, UserLoginBean>() {
            private ValueState<String> uvstate;

            @Override
            public void open(Configuration parameters)   {
                ValueStateDescriptor<String> uvstate1 = new ValueStateDescriptor<String>("uvstate", String.class);
                uvstate1.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                uvstate = getRuntimeContext().getState(uvstate1);


            }

            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, UserLoginBean>.Context context, Collector<UserLoginBean> collector) throws Exception {
                String uv_state = uvstate.value();
                Long ts1 = jsonObject.getLong("ts");
                String s = dataformtutil.tsToDate(ts1);
                Long uv = 0L;
                Long hl = 0L;


//                运用 Flink 状态编程，记录用户末次登录日期。
                if (StringUtils.isNotEmpty(uv_state)) {
                    //若状态中的末次登录日期不为 null，进一步判断。
//如果末次登录日期不等于当天日期则独立用户数 uuCt 记为 1，并将状态中的末次登录日期更新为当日，进一步判断。
                    if (StringUtils.isNotEmpty(s)) {
                        uv = 1L;
                        uvstate.update(s);
                        //                如果当天日期与末次登录日期之差大于等于8天则回流用户数backCt置为1。
//                否则 backCt 置为 0。
//若末次登录日期为当天，则 uuCt 和 backCt 均为 0，此时本条数据不会影响统计结果，舍弃，不再发往下游。
                        Long day = (ts1 - dataformtutil.dateToTs(uv_state)) / 1000 / 60 / 60 / 24;
                        if (day > 8) {
                            hl = 1L;

                        }
                    }

                } else {
                    //如果状态中的末次登录日期为 null将 uuCt 置为 1，
                    // backCt 置为 0，并将状态中的末次登录日期更新为当日。

                    uv = 1L;
                    hl = 0L;
                    uvstate.update(s);

                }
                if (uv != 0L || hl != 0L) {
                    collector.collect(new UserLoginBean("", "", "", uv, hl, ts1));
                }

            }
        });
//        process.print();/
//        2> UserLoginBean(stt=, edt=, curDate=, backCt=1, uuCt=0, ts=1744810608868)
        //水位线
        SingleOutputStreamOperator<UserLoginBean> userLoginBeanSingleOutputStreamOperator = process.assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserLoginBean>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserLoginBean>() {
                            @Override
                            public long extractTimestamp(UserLoginBean jsonObject, long l) {
                                return jsonObject.getTs();
                            }
                        })
        );

        //开窗
        AllWindowedStream<UserLoginBean, TimeWindow> userLoginBeanTimeWindowAllWindowedStream = userLoginBeanSingleOutputStreamOperator.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.hours(3)));
        // 聚合
        SingleOutputStreamOperator<UserLoginBean> reduce = userLoginBeanTimeWindowAllWindowedStream.reduce(
                new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean a, UserLoginBean aa)   {
                        a.setBackCt(aa.getBackCt());
                        a.setUuCt(aa.getUuCt());
                        return null;
                    }
                }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<UserLoginBean> iterable, Collector<UserLoginBean> collector) throws Exception {
                        UserLoginBean next = iterable.iterator().next();
                        String stt = dataformtutil.tsToDateTime(timeWindow.getStart());
                        String edt = dataformtutil.tsToDateTime(timeWindow.getEnd());
                        String curDate = dataformtutil.tsToDate(timeWindow.getStart());
                        next.setStt(stt);
                        next.setEdt(edt);
                        next.setCurDate(curDate);
                        collector.collect(next);
                    }
                }
        );

//        reduce.print();
//        3> UserLoginBean(stt=2025-04-16 14:00:00, edt=2025-04-16 17:00:00, curDate=2025-04-16, backCt=1, uuCt=0, ts=1744788137467)

        //转成json
        //写入doris
        SingleOutputStreamOperator<String> map1 = reduce.map(new MapFunction<UserLoginBean, String>() {
            @Override
            public String map(UserLoginBean userLoginBean)   {
                return JSON.toJSONString(userLoginBean);
            }
        });
//        map1.print();

        map1.sinkTo(finksink.getDorisSink("dws_user_user_login_window"));
    env.execute();
    }
}
