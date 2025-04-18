package com.cj.realtime.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cj.realtime.base.BaseApp;
import com.cj.realtime.bean.TableProcessDim;
import com.cj.realtime.bean.TableProcessDwd;
import com.cj.realtime.constant.Constant;
import com.cj.realtime.util.FlinkSinkUtil;
import com.cj.realtime.util.FlinkSourceUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.shell.Concat;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

/**
 * @Package com.cj.realtime.dwd.DwdBaseDb
 * @Author chen.jian
 * @Date 2025/4/13 19:06
 * @description: 开发思路分析
 */
public class DwdBaseDb  {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh02:9092")
                .setTopics("topic_db")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");


        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(s);
                            String type = jsonObj.getString("op");
                            if (!type.startsWith("bootstrap-")) {
                                out.collect(jsonObj);
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("不是一个标准的json");
                        }
                    }
                }
        );

////        {"op":"c","after":{"create_time":1743882806000,"user_id":61,"appraise":"1201","comment_txt":"评论内容：28826173848582321958685647693595672261387449888379","nick_name":"筠筠","sku_id":15,"id":94,"spu_id":4,"order_id":1048},"source":{"thread":122,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000007","connector":"mysql","pos":5242875,"name":"mysql_binlog_source","row":0,"ts_ms":1744544393000,"snapshot":"false","db":"gmall_config","table":"comment_info"},"ts_ms":1744544393753}
////        {"op":"u","before":{"is_ordered":0,"cart_price":9199.0,"sku_num":1,"create_time":1743884105000,"user_id":"310","sku_id":18,"sku_name":"TCL 75Q10 75英寸 QLED原色量子点电视 安桥音响 AI声控智慧屏 超薄全面屏 MEMC防抖 3+32GB 平板电视","id":1091},"after":{"is_ordered":1,"cart_price":9199.0,"sku_num":1,"create_time":1743884105000,"user_id":"310","sku_id":18,"sku_name":"TCL 75Q10 75英寸 QLED原色量子点电视 安桥音响 AI声控智慧屏 超薄全面屏 MEMC防抖 3+32GB 平板电视","id":1091,"order_time":1743884138000,"operate_time":1743884138000},"source":{"thread":109,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000007","connector":"mysql","pos":5243296,"name":"mysql_binlog_source","row":0,"ts_ms":1744544393000,"snapshot":"false","db":"gmall_config","table":"cart_info"},"ts_ms":1744544393753}
////        {"op":"u","before":{"is_ordered":0,"cart_price":6699.0,"sku_num":1,"create_time":1743884126000,"user_id":"310","sku_id":17,"sku_name":"TCL 65Q10 65英寸 QLED原色量子点电视 安桥音响 AI声控智慧屏 超薄全面屏 MEMC防抖 3+32GB 平板电视","id":1093},"after":{"is_ordered":1,"cart_price":6699.0,"sku_num":1,"create_time":1743884126000,"user_id":"310","sku_id":17,"sku_name":"TCL 65Q10 65英寸 QLED原色量子点电视 安桥音响 AI声控智慧屏 超薄全面屏 MEMC防抖 3+32GB 平板电视","id":1093,"order_time":1743884138000,"operate_time":1743884138000},"source":{"thread":109,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000007","connector":"mysql","pos":5246064,"name":"mysql_binlog_source","row":0,"ts_ms":1744544393000,"snapshot":"false","db":"gmall_config","table":"cart_info"},"ts_ms":1744544393756}


        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("gmall2025_config", "table_process_dwd");
        DataStreamSource<String> mysqStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");
        SingleOutputStreamOperator<TableProcessDwd> tpDS = mysqStrDS.map(
                new MapFunction<String, TableProcessDwd>() {
                    @Override
                    public TableProcessDwd map(String s) throws Exception {

                        JSONObject object = JSON.parseObject(s);
                        String op = object.getString("op");
                        TableProcessDwd tp = null;
                        if ("d".equals(op)) {
                            tp = object.getObject("before", TableProcessDwd.class);
                        } else {
                            tp = object.getObject("after", TableProcessDwd.class);
                        }
                        tp.setOp(op);
                        return tp;
                    }
                }
        );
//        tpDS.print();
//        TableProcessDwd(sourceTable=favor_info, sourceType=insert, sinkTable=dwd_interaction_favor_add, sinkColumns=id,user_id,sku_id,create_time, op=r)
//        TableProcessDwd(sourceTable=coupon_use, sourceType=update, sinkTable=dwd_tool_coupon_use, sinkColumns=id,coupon_id,user_id,order_id,using_time,used_time,coupor, op=r)

        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcessDwd>("mapStateDescriptor",String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDS = tpDS.broadcast(mapStateDescriptor);

        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDS = jsonObjDS.connect(broadcastDS);

        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDwd>> splitDS = connectDS.process(
                new BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject,TableProcessDwd>>() {
                    private Map<String,TableProcessDwd> configMap = new HashMap<>();
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        Class.forName("com.mysql.cj.jdbc.Driver");
                        java.sql.Connection conn = DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
                        String sql = "select * from gmall2025_config.table_process_dwd";
                        PreparedStatement ps = conn.prepareStatement(sql);
                        ResultSet rs = ps.executeQuery();
                        ResultSetMetaData metaData = rs.getMetaData();
                        while (rs.next()){
                            JSONObject jsonObj = new JSONObject();
                            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                                String columnName = metaData.getColumnName(i);
                                Object columnValue = rs.getObject(i);
                                jsonObj.put(columnName,columnValue);
                            }
                            TableProcessDwd tableProcessDim = jsonObj.toJavaObject(TableProcessDwd.class);
                            configMap.put(tableProcessDim.getSourceTable(), tableProcessDim);
                        }

                        rs.close();
                        ps.close();
                        conn.close();
                    }
                    private String getKey(String sourceTable, String sourceType) {
                        String key = sourceTable + ":" + sourceType;
                        return key;
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject,TableProcessDwd>>.ReadOnlyContext ctx, Collector<Tuple2<JSONObject,TableProcessDwd>> out) throws Exception {

                        String table = jsonObj.getJSONObject("source").getString("table");
                        String op = jsonObj.getString("op");
                        ReadOnlyBroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        TableProcessDwd tableProcessDim = broadcastState.get(table);
                        if (tableProcessDim != null){
                            JSONObject dataJsonObj = jsonObj.getJSONObject("after");
                            String sinkColumns = tableProcessDim.getSinkColumns();

                            deleteNoeedColumns(dataJsonObj,sinkColumns);
                            Long ts = jsonObj.getLong("ts_ms");
                            dataJsonObj.put("ts_ms",ts);
                            out.collect(Tuple2.of(dataJsonObj,tableProcessDim));
                        }

                    }

                    @Override
                    public void processBroadcastElement(TableProcessDwd tp, BroadcastProcessFunction<JSONObject, TableProcessDwd, Tuple2<JSONObject,TableProcessDwd>>.Context ctx, Collector<Tuple2<JSONObject,TableProcessDwd>> out) throws Exception {
                        String op = tp.getOp();

                        BroadcastState<String, TableProcessDwd> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        String sourceTable = tp.getSourceTable();
                        if ("d".equals(op)){
                            broadcastState.remove(sourceTable);
                            configMap.remove(sourceTable);
                        }else {
                            broadcastState.put(sourceTable,tp);
                            configMap.put(sourceTable,tp);
                        }
                    }
                }
        );
        splitDS.print();

//        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> splitDS = connectDS.process(new BaseDbTableProcessFunction(mapStateDescriptor));
//        splitDS.print();

        splitDS.sinkTo(FlinkSinkUtil.getKafkaSink());
        env.execute();
    }
    private static void deleteNoeedColumns(JSONObject dataJsonObj, String sinkColumns) {
        List<String> columnList = Arrays.asList(sinkColumns.split(","));

        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

        entrySet.removeIf(entry-> !columnList.contains(entry.getKey()));
    }

}
