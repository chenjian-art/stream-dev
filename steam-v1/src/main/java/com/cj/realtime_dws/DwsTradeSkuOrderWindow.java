package com.cj.realtime_dws;
import com.cj.Base.BaseApp;
import com.cj.bean.TradeSkuOrderBean;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cj.constat.constat;
import lombok.SneakyThrows;
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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;
import com.cj.utils.Hbaseutli;
import com.cj.utils.dataformtutil;
import com.cj.utils.finksink;

import java.math.BigDecimal;

/**
 * @Package com.cj.realtime_dws.DwsTradeSkuOrderWindow
 * @Author chen.jian
 * @Date 2025/4/15 14:49
 * @description: sku粒度下单业务过程聚合统计
 */
public class DwsTradeSkuOrderWindow {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.days(30), org.apache.flink.api.common.time.Time.seconds(3)));

//        获取数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("dwd_trade_order_detail")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<JSONObject> process = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector){
                JSONObject jsonObject = JSON.parseObject(s);
                if (jsonObject != null) {
                    collector.collect(jsonObject);
                }
            }
        });

//        process.print();
//        2> {"id":"2347","order_id":"1478","user_id":"220","sku_id":"2","sku_name":"小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+512GB 冷杉绿 5G手机","province_id":"1","activity_id":"1","activity_rule_id":"1","coupon_id":null,"date_id":null,"create_time":"1744411542000","sku_num":"1","split_original_amount":"6999.0000","split_activity_amount":"500.00","split_coupon_amount":"0.00","split_total_amount":"6499.00","ts_ms":1744554510363}
        //TODO 2.按照唯一键(订单明细的id)进行分组
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = process.keyBy(o->o.getString("id"));
//        orderDetailIdKeyedDS.print();
//        2> {"create_time":"1744412806000","sku_num":"1","split_original_amount":"999.0000","split_coupon_amount":"0.00","sku_id":"4","user_id":"474","province_id":"28","sku_name":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 4GB+128GB 冰雾白 游戏智能手机 小米 红米","id":"2363","order_id":"1491","split_activity_amount":"0.00","ts_ms":1744554510367,"split_total_amount":"999.00"}

        //TODO  去重
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters){
                        ValueStateDescriptor<JSONObject> valueStateDescriptor
                                = new ValueStateDescriptor<JSONObject>("lastJsonObjState", JSONObject.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //从状态中获取上次接收到的数据
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj != null) {
                            //说明重复了 ，将已经发送到下游的数据(状态)，影响到度量值的字段进行取反再传递到下游
                            String splitOriginalAmount = lastJsonObj.getString("split_original_amount");
                            String splitCouponAmount = lastJsonObj.getString("split_coupon_amount");
                            String splitActivityAmount = lastJsonObj.getString("split_activity_amount");
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");

                            lastJsonObj.put("split_original_amount", "-" + splitOriginalAmount);
                            lastJsonObj.put("split_coupon_amount", "-" + splitCouponAmount);
                            lastJsonObj.put("split_activity_amount", "-" + splitActivityAmount);
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            out.collect(lastJsonObj);
                        }
                        lastJsonObjState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );

//        distinctDS.print();
//        e_id":"16","activity_id":"1","sku_name":"小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机","id":"2377","order_id":"1504","split_activity_amount":"-500.00","ts_ms":1744419908467,"split_total_amount":"-5999.00"}
//        2> {"create_time":"1744329706000","sku_num":"1","activity_rule_id":"1","split_original_amount":"6499.0000","split_coupon_amount":"0.00","sku_id":"3","user_id":"478","province_id":"16","activity_id":"1","sku_name":"小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机","id":"2377","order_id":"1504","split_activity_amount":"500.00","ts_ms":1744554510394,"split_total_amount":"5999.00"}
//        2> {"create_time":"1744329706000","sku_num":"1","activity_rule_id":"1","split_original_amount":"-6499.0000","split_coupon_amount":"-0.00","sku_id":"3","user_id":"478","province_id":"16","activity_id":"1","sku_name":"小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机","id":"2377","order_id":"1504","split_activity_amount":"-500.00","ts_ms":1744554510394,"split_total_amount":"-5999.00"}
//        2> {"create_time":"1744329706000","sku_num":"1","activity_rule_id":"1","split_original_amount":"6499.0000","split_coupon_amount":"0.00","sku_id":"3","user_id":"478","province_id":"16","activity_id":"1","sku_name":"小米12S Ultra 骁龙8+旗舰处理器 徕卡光学镜头 2K超视感屏 120Hz高刷 67W快充 12GB+256GB 经典黑 5G手机","id":"2377","order_id":"1504","split_activity_amount":"500.00","ts_ms":1744554510394,"split_total_amount":"5999.00"}

//        //TODO 4.指定Watermark以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts");
                                    }
                                }
                        )
        );
//        withWatermarkDS.print();
//        er_id":"78","province_id":"6","activity_id":"4","sku_name":"TCL 85Q6 85英寸 巨幕私人影院电视 4K超高清 AI智慧屏 全景全面屏 MEMC运动防抖 2+16GB 液晶平板电视机","id":"237","order_id":"140","split_activity_amount":"-1199.90","ts_ms":1744419906878,"split_total_amount":"-10799.10"}

//        //TODO 5.再次对流中数据进行类型转换  jsonObj->统计的实体类对象
        SingleOutputStreamOperator<TradeSkuOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(JSONObject jsonObj) {
                        //{"create_time":"2024-06-11 10:54:40","sku_num":"1","activity_rule_id":"5","split_original_amount":"11999.0000",
                        // "split_coupon_amount":"0.0","sku_id":"19","date_id":"2024-06-11","user_id":"2998","province_id":"32",
                        // "activity_id":"4","sku_name":"TCL","id":"15183","order_id":"10788","split_activity_amount":"1199.9",
                        // "split_total_amount":"10799.1","ts":1718160880}
                        String skuId = jsonObj.getString("sku_id");
                        BigDecimal splitOriginalAmount = jsonObj.getBigDecimal("split_original_amount");
                        BigDecimal splitCouponAmount = jsonObj.getBigDecimal("split_coupon_amount");
                        BigDecimal splitActivityAmount = jsonObj.getBigDecimal("split_activity_amount");
                        BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                        Long ts = jsonObj.getLong("ts") ;
                        TradeSkuOrderBean orderBean = TradeSkuOrderBean.builder()
                                .skuId(skuId)
                                .originalAmount(splitOriginalAmount)
                                .couponReduceAmount(splitCouponAmount)
                                .activityReduceAmount(splitActivityAmount)
                                .orderAmount(splitTotalAmount)
                                .ts(ts)
                                .build();
                        return orderBean;
                    }
                }
        );

//        beanDS.print();
//        3> TradeSkuOrderBean(stt=null, edt=null, curDate=null, trademarkId=null, trademarkName=null, category1Id=null, category1Name=null, category2Id=null, category2Name=null, category3Id=null, category3Name=null, skuId=6, skuName=null, spuId=null, spuName=null, originalAmount=-2598.0000, activityReduceAmount=0.00, couponReduceAmount=0.00, orderAmount=-2598.00, ts=1744595916797)

//        //TODO 6.分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS = beanDS.keyBy(TradeSkuOrderBean::getSkuId);
//        skuIdKeyedDS.print();
//        2> TradeSkuOrderBean(stt=null, edt=null, curDate=null, trademarkId=null, trademarkName=null, category1Id=null, category1Name=null, category2Id=null, category2Name=null, category3Id=null, category3Name=null, skuId=35, skuName=null, spuId=null, spuName=null, originalAmount=-5499.0000, activityReduceAmount=0.00, couponReduceAmount=0.00, orderAmount=-5499.00, ts=1744419908420)

//        //TODO 7.开窗
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS = skuIdKeyedDS.window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

//        //TODO 8.聚合
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2)  {
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                        value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out)  {
                        TradeSkuOrderBean orderBean = elements.iterator().next();
                        TimeWindow window = context.window();
                        String stt = dataformtutil.tsToDateTime(window.getStart());
                        String edt = dataformtutil.tsToDateTime(window.getEnd());
                        String curDate = dataformtutil.tsToDate(window.getStart());
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        out.collect(orderBean);
                    }
                }
        );
//        reduceDS.print();
//        2> TradeSkuOrderBean(stt=2025-04-16 14:40:50, edt=2025-04-16 14:41:00, curDate=2025-04-16, trademarkId=null, trademarkName=null, category1Id=null, category1Name=null, category2Id=null, category2Name=null, category3Id=null, category3Name=null, skuId=31, skuName=null, spuId=null, spuName=null, originalAmount=8763.0000, activityReduceAmount=0.00, couponReduceAmount=204.61, orderAmount=8558.39, ts=1744205865833)

//
//        //TODO 9.关联sku维度
//        /*
//        //维度关联最基本的实现方式
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = reduceDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
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
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) {
                        String skuId = orderBean.getSkuId();
                        JSONObject skuInfoJsonObj = Hbaseutli.getRow(hbaseConn,constat.HBASE_NAMESPACE, "dim_sku_info", skuId, JSONObject.class);
                        orderBean.setSkuName(skuInfoJsonObj.getString("sku_name"));
                        orderBean.setSpuId(skuInfoJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(skuInfoJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(skuInfoJsonObj.getString("tm_id"));
                        return orderBean;
                    }
                }
        );
        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = withSkuInfoDS.map(

                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {

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
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean)  {
                        String spuId = orderBean.getSpuId();
                        JSONObject skuInfoJsonObj = Hbaseutli.getRow(hbaseConn, constat.HBASE_NAMESPACE, "dim_spu_info", spuId, JSONObject.class);
                        orderBean.setSpuName(skuInfoJsonObj.getString("spu_name"));
                        return orderBean;
                    }
                }
        );

        SingleOutputStreamOperator<TradeSkuOrderBean> withTmDS = withSpuInfoDS.map(

                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {

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
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) {
                        String spuId = orderBean.getTrademarkId();
                        JSONObject skuInfoJsonObj = Hbaseutli.getRow(hbaseConn, constat.HBASE_NAMESPACE, "dim_base_trademark", spuId, JSONObject.class);
                        orderBean.setTrademarkName(skuInfoJsonObj.getString("tm_name"));
                        return orderBean;
                    }
                }
        );

        SingleOutputStreamOperator<TradeSkuOrderBean> c3Stream = withTmDS.map(

                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {

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
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean)   {
                        String spuId = orderBean.getCategory3Id();
                        JSONObject skuInfoJsonObj = Hbaseutli.getRow(hbaseConn, constat.HBASE_NAMESPACE, "dim_base_category3", spuId, JSONObject.class);
                        orderBean.setCategory3Name(skuInfoJsonObj.getString("name"));
                        orderBean.setCategory2Id(skuInfoJsonObj.getString("category2_id"));
                        return orderBean;
                    }
                }
        );
        SingleOutputStreamOperator<TradeSkuOrderBean> c2Stream = c3Stream.map(

                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {

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
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean)   {
                        String spuId = orderBean.getCategory2Id();
                        JSONObject skuInfoJsonObj = Hbaseutli.getRow(hbaseConn, constat.HBASE_NAMESPACE, "dim_base_category2", spuId, JSONObject.class);
                        orderBean.setCategory2Name(skuInfoJsonObj.getString("name"));
                        orderBean.setCategory1Id(skuInfoJsonObj.getString("category1_id"));
                        return orderBean;
                    }
                }
        );
        SingleOutputStreamOperator<TradeSkuOrderBean> c1Stream = c2Stream.map(

                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {

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
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean)   {
                        String spuId = orderBean.getCategory1Id();
                        JSONObject skuInfoJsonObj = Hbaseutli.getRow(hbaseConn, constat.HBASE_NAMESPACE, "dim_base_category1", spuId, JSONObject.class);
                        orderBean.setCategory1Name(skuInfoJsonObj.getString("name"));
                        return orderBean;
                    }
                }
        );

        c1Stream.print();
        SingleOutputStreamOperator<String> map = c1Stream.map(new RichMapFunction<TradeSkuOrderBean, String>() {
            @Override
            public String map(TradeSkuOrderBean tradeSkuOrderBean)   {

                return JSON.toJSONString(tradeSkuOrderBean);
            }
        });
//        map.print();

        map.sinkTo(finksink.getDorisSink("dws_trade_sku_order_window"));
    env.execute();
    }
}
