package com.cj.realtime_dws;
import com.cj.Base.BaseApp;
import com.cj.bean.TrafficHomeDetailPageViewBean;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cj.constat.constat;
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

import java.time.Duration;

/**
 * @Package com.cj.realtime_dws.DwsTrafficHomeDetailPageViewWindow
 * @Author chen.jian
 * @Date 2025/4/15 9:35
 * @description: 首页、详情页独立访客聚合统计
 */

public class DwsTrafficHomeDetailPageViewWindow  {
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
//      获取数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("dwd_traffic_page")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        //TODO 1.对流中数据类型进行转换   jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        //TODO 2.过滤首页以及详情页
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        return "home".equals(pageId) || "good_detail".equals(pageId);
                    }
                }
        );
//        filterDS.print();
//        2> {"common":{"ar":"26","uid":"292","os":"Android 13.0","ch":"wandoujia","is_new":"0","md":"vivo x90","mid":"mid_51","vc":"v2.1.134","ba":"vivo","sid":"e76725db-cf1e-4fa7-9ae5-a8b56635afaa"},"page":{"from_pos_seq":1,"page_id":"good_detail","item":"12","during_time":11211,"item_type":"sku_id","last_page_id":"good_detail","from_pos_id":4},"ts":1744728156258}
       //按照mid分组
        KeyedStream<JSONObject, String> jsonObjectStringKeyedStream = filterDS.keyBy(o -> o.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> process = jsonObjectStringKeyedStream.process(new ProcessFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
            private ValueState<String> home;
            private ValueState<String> detail;

            @Override
            public void open(Configuration parameters)   {
                ValueStateDescriptor<String> homeValueStateDescriptor
                        = new ValueStateDescriptor<String>("home", String.class);

                homeValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                home = getRuntimeContext().getState(homeValueStateDescriptor);


                ValueStateDescriptor<String> detail1 = new ValueStateDescriptor<String>("detail", String.class);

                detail1.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                detail = getRuntimeContext().getState(detail1);


            }
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, TrafficHomeDetailPageViewBean>.Context context, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                String pageId = jsonObject.getJSONObject("page").getString("page_id");

                Long ts = jsonObject.getLong("ts");
                String curVisitDate = dataformtutil.tsToDate(ts);
                Long homeUvCt = 0L;
                Long detailUvCt = 0L;

                if ("home".equals(pageId)) {
                    //获取首页的上次访问日期
                    String homeLastVisitDate = home.value();
                    if (StringUtils.isEmpty(homeLastVisitDate) || !homeLastVisitDate.equals(curVisitDate)) {
                        homeUvCt = 1L;
                        home.update(curVisitDate);
                    }
                } else {
                    //获取详情页的上次访问日期
                    String detailLastVisitDate = detail.value();
                    if (StringUtils.isEmpty(detailLastVisitDate) || !detailLastVisitDate.equals(curVisitDate)) {
                        detailUvCt = 1L;
                        detail.update(curVisitDate);
                    }
                }
                //这什么业务   首页和详情页
                if (homeUvCt != 0L || detailUvCt != 0L) {
                    collector.collect(new TrafficHomeDetailPageViewBean(
                            "", "", "", homeUvCt, detailUvCt, ts
                    ));
                }
            }
        });
//        process.print();
        //水
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> swx = process.
                assignTimestampsAndWatermarks(WatermarkStrategy.<TrafficHomeDetailPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(new SerializableTimestampAssigner<TrafficHomeDetailPageViewBean>() {
            @Override
            public long extractTimestamp(TrafficHomeDetailPageViewBean trafficHomeDetailPageViewBean, long l) {
                return trafficHomeDetailPageViewBean.getTs();
            }
        }));
//        4> TrafficHomeDetailPageViewBean(stt=, edt=, curDate=, homeUvCt=0, goodDetailUvCt=1, ts=1744722384856)
// 乱序三秒
////         //TODO 6.开窗
        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowDS = swx.
                windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(3)));
// TODO 7.聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2)   {
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        return value1;
                    }
                },
                new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        TrafficHomeDetailPageViewBean viewBean = values.iterator().next();
                        String stt = dataformtutil.tsToDateTime(window.getStart());
                        String edt = dataformtutil.tsToDateTime(window.getEnd());
                        String curDate = dataformtutil.tsToDate(window.getStart());
                        viewBean.setStt(stt);
                        viewBean.setEdt(edt);
                        viewBean.setCurDate(curDate);
                        out.collect(viewBean);
                    }
                }
        );
        //TODO 8.将聚合的结果写到Doris
//        reduceDS.print();
//        4> TrafficHomeDetailPageViewBean(stt=2025-04-16 23:11:57, edt=2025-04-16 23:12:00, curDate=2025-04-16, homeUvCt=0, goodDetailUvCt=1, ts=1744816317782)

        SingleOutputStreamOperator<String> map = reduceDS.map(new MapFunction<TrafficHomeDetailPageViewBean, String>() {
            @Override
            public String map(TrafficHomeDetailPageViewBean trafficHomeDetailPageViewBean)   {
                return com.alibaba.fastjson2.JSON.toJSONString(trafficHomeDetailPageViewBean);
            }
        });
        map.print();
//        1> {"curDate":"2025-04-16","edt":"2025-04-16 23:09:21","goodDetailUvCt":1,"homeUvCt":0,"stt":"2025-04-16 23:09:18"}
//        Caused by: org.apache.doris.flink.exception.DorisRuntimeException: tabel {} stream load error: realtime_v1.DwsTrafficHomeDetailPageViewWindow, see more in [CANCELLED][DATA_QUALITY_ERROR]Encountered unqualified data, stop processing

        map.sinkTo(finksink.getDorisSink("dws_traffic_home_detail_page_view_window"));
    env.execute();
    }

}
