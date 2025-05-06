package com.cj.realtime_dws;
import com.cj.Base.BaseApp;
import com.cj.bean.CartADDUU;
import com.cj.constat.constat;
import org.apache.flink.api.common.functions.MapFunction;
import com.cj.utils.dataformtutil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
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
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import com.cj.utils.finksink;

/**
 * @Package com.cj.realtime_dws.DwsTradeCartAddUuWindow
 * @Author chen.jian
 * @Date 2025/4/15 11:47
 * @description: 加购独立用户统计
 */
public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.days(30), org.apache.flink.api.common.time.Time.seconds(3)));

//        获取kafka
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("dwd_trade_cart_add")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

            //转换类型
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

//        分组
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts") ;
                                    }
                                }
                        )
        );

//        按照用户的id进行分组
        KeyedStream<JSONObject, String> keyiby = withWatermarkDS.keyBy(o -> o.getString("user_id"));


        //状态编程  判断是否是加购独立用户
        SingleOutputStreamOperator<JSONObject> process = keyiby.process(
            new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                private ValueState<String> lastCartDateState;

                @Override
                public void open(Configuration parameters)   {
                    ValueStateDescriptor<String> valueStateDescriptor = new
                            ValueStateDescriptor<>("lastCartDateState", String.class);
                    valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                    lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);

                }
                @Override
                public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                    String lastCartDate = lastCartDateState.value();

                    Long ts1 = jsonObject.getLong("ts");
                    String curCartDate = dataformtutil.tsToDate(ts1);
                    if (StringUtils.isEmpty(lastCartDate) || !lastCartDate.equals(curCartDate)) {
                        collector.collect(jsonObject);
                        lastCartDateState.update(curCartDate);
                    }
                }
            }
        );


        SingleOutputStreamOperator<JSONObject> ts = process.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        })
        );
        //开窗
        AllWindowedStream<JSONObject, TimeWindow> jsonObjectTimeWindowAllWindowedStream = ts.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)));
        // 聚合
        SingleOutputStreamOperator<CartADDUU> aggregate = jsonObjectTimeWindowAllWindowedStream.aggregate(
                new AggregateFunction<JSONObject, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(JSONObject value, Long accumulator) {
                        return accumulator+1;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return a+b;
                    }
                },
                new AllWindowFunction<Long, CartADDUU, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Long> iterable, Collector<CartADDUU> collector)   {
                        Long uuCt = iterable.iterator().next();
                        String stt = dataformtutil.tsToDateTime(timeWindow.getStart());
                        String edt = dataformtutil.tsToDateTime(timeWindow.getEnd());
                        String curDate = dataformtutil.tsToDate(timeWindow.getStart());
                        collector.collect(new CartADDUU(stt, edt, curDate, uuCt));
                    }
                }
        );
        aggregate.print();
        // 写入doris


        SingleOutputStreamOperator<String> map1 = aggregate.map(new MapFunction<CartADDUU, String>() {
            @Override
            public String map(CartADDUU cartADDUU)   {
                return JSON.toJSONString(cartADDUU);
            }
        });
//        map1.print();


        map1.sinkTo(finksink.getDorisSink("dws_trade_cart_add_uu_window"));
    env.execute();
    }
}

