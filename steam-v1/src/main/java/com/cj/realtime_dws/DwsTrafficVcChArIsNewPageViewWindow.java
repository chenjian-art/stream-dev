package com.cj.realtime_dws;
import com.cj.Base.BaseApp;
import com.cj.bean.TrafficPageViewBean;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cj.constat.constat;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import com.cj.utils.dataformtutil;

import java.time.Duration;

import com.cj.utils.finksink;

/**
 * @Package com.cj.realtime.dws.DwsTrafficVcChArIsNewPageViewWindow
 * @Author chen.jian
 * @Date 2025/4/14 14:33
 * @description: 按照版本、地区、渠道、新老访客对pv、uv、sv、dur进行聚合统计
 */


public class DwsTrafficVcChArIsNewPageViewWindow  {
    public static  void main(String[] args) throws Exception {

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
//        数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("dwd_traffic_page")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
//        类型转换
        SingleOutputStreamOperator<JSONObject> data_v1 = kafkaStrDS.map(JSON::parseObject);
//        mid分组
        KeyedStream<JSONObject, String> keyedStream = data_v1.keyBy(o -> o.getJSONObject("common").getString("mid"));
//        判断是否是新用户
        SingleOutputStreamOperator<TrafficPageViewBean> beanDS = keyedStream.map(
                new RichMapFunction<JSONObject, TrafficPageViewBean>() {
                    private ValueState<String> lastVisitDateState;
                    @Override
                    public void open(Configuration parameters)   {
                        ValueStateDescriptor<String> lastVisitDateState1 = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                        lastVisitDateState1.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                            lastVisitDateState=getRuntimeContext().getState(lastVisitDateState1);

                    }

                    @Override
                    public TrafficPageViewBean map(JSONObject jsonObj) throws Exception {
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");

                        //从状态中获取当前设置上次访问日期
                        String lastVisitDate = lastVisitDateState.value();
                        //获取当前访问日期
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = dataformtutil.tsToDate(ts);
                        Long uvCt = 0L;
                        if(StringUtils.isEmpty(lastVisitDate) || !lastVisitDate.equals(curVisitDate)){
                            uvCt = 1L;
                            lastVisitDateState.update(curVisitDate);
                        }
                        String lastPageId = pageJsonObj.getString("last_page_id");
                        Long svCt = StringUtils.isEmpty(lastPageId) ? 1L : 0L ;
                        return new TrafficPageViewBean(
                                "",
                                "",
                                "",
                                commonJsonObj.getString("vc"),
                                commonJsonObj.getString("ch"),
                                commonJsonObj.getString("ar"),
                                commonJsonObj.getString("is_new"),
                                uvCt,
                                svCt,
                                1L,
                                pageJsonObj.getLong("during_time"),
                                ts
                        );
                    }
                }
        );
//        beanDS.print();

//        水位线
        SingleOutputStreamOperator<TrafficPageViewBean> trafficPageViewBeanSingleOutputStreamOperator = beanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.
                        <TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                            @Override
                            public long extractTimestamp(TrafficPageViewBean trafficPageViewBean, long l) {
                                return trafficPageViewBean.getTs();

                            }
                        })

        );
//        维度分组
        KeyedStream<TrafficPageViewBean, org.apache.flink.api.java.tuple.Tuple4<String, String, String, String>> dimKeyedDS = trafficPageViewBeanSingleOutputStreamOperator.keyBy(
                new KeySelector<TrafficPageViewBean, org.apache.flink.api.java.tuple.Tuple4<String, String, String, String>>() {
                    @Override
                    public org.apache.flink.api.java.tuple.Tuple4<String, String, String, String> getKey(TrafficPageViewBean bean)   {
                        return Tuple4.of(bean.getVc(),
                                bean.getCh(),
                                bean.getAr(),
                                bean.getIsNew());
                    }
                }
        );
//        dimKeyedDS.print();
//        开窗
        WindowedStream<TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDS
                = dimKeyedDS.window(TumblingEventTimeWindows.
                of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

//        3> TrafficPageViewBean(stt=, edt=, cur_date=, vc=v2.1.111, ch=xiaomi, ar=13, isNew=1, uvCt=0, svCt=0, pvCt=1, durSum=13771, ts=1744038466285)
//        聚合
        SingleOutputStreamOperator<TrafficPageViewBean> result = windowDS
                // 定义一个时间窗口，这里假设是 5 分钟的滚动窗口，你可以根据实际需求修改
                .reduce(
                        new ReduceFunction<TrafficPageViewBean>() {
                            @Override
                            public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2)   {
                                value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                                value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                                value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                                value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                                return value1;
                            }
                        },
                        new WindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                            @Override
                            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow timeWindow, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) throws Exception {
                                TrafficPageViewBean pageViewBean = iterable.iterator().next();
                                String s = dataformtutil.tsToDateTime(timeWindow.getStart());
                                String s1 = dataformtutil.tsToDateTime(timeWindow.getEnd());
                                String s2 = dataformtutil.tsToDate(timeWindow.getStart());
                                pageViewBean.setStt(s);
                                pageViewBean.setEdt(s1);
                                pageViewBean.setCur_date(s2);
                                collector.collect(pageViewBean);
                            }
                        }
                );
        result.print();


        SingleOutputStreamOperator<String> map = result
                .map(new MapFunction<TrafficPageViewBean, String>() {

                    @Override
                    public String map(TrafficPageViewBean trafficPageViewBean)   {
                     return JSON.toJSONString(trafficPageViewBean);
                    }
                });
//        写入doris
        map.sinkTo(finksink.getDorisSink("dws_traffic_vc_ch_ar_is_new_page_view_window"));
    env.execute();
    }

}
