package com.cj.realtime_Dwd;

import com.cj.Base.BaseApp;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.cj.constat.constat;
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
import com.cj.bean.TableProcessDwd;
import com.cj.realtime_Dim.flinkfcation.flinksorceutil;
import com.cj.realtime_Dwd.function.BaseDbTableProcessFunction;
import com.cj.utils.finksink;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

/**
 * @Package com.cj.realtime_Dwd.DwdBaseDb
 * @Author chen.jian
 * @Date 2025/4/10 上午11:44
 * @description: 日志分流
 */
//数据已经跑了重新

public class DwdBaseDb  {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        读取kafka数据
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("cdh02:9092")
            .setTopics("topic_db")
            .setGroupId("my-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStreamSource<String> kafkaStrDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

//        数据类型转换并进行ETL
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
            new ProcessFunction<String, JSONObject>() {
                @Override
                public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out)   {
                    try {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
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

//        jsonObjDS.print();

//        读取mysql配置表信息
        MySqlSource<String> mySqlSource = flinksorceutil.getmysqlsource("gmall2025_config","table_process_dwd");
//            读取数据 封装为流
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source");

//        数据转换成实体类对象
        SingleOutputStreamOperator<TableProcessDwd> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDwd>() {
                    @Override
                    public TableProcessDwd map(String jsonStr)   {

//                        为了处理方便，先将jsonStr转换为jsonObj
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
//                        获取操作类型
                        String op = jsonObj.getString("op");
                        TableProcessDwd tp = null;
                        if("d".equals(op)){
//                            获取删除前配置信息
                            tp = jsonObj.getObject("before", TableProcessDwd.class);
                        }else{
                            //获取配置信息
                            tp = jsonObj.getObject("after", TableProcessDwd.class);
                        }
                        tp.setOp(op);
                        return tp;
                    }
                }
        );

//        tpDS.print();

//        对配置流进行广播 ---broadcast
        MapStateDescriptor<String, TableProcessDwd> mapStateDescriptor
                = new MapStateDescriptor<String, TableProcessDwd>
                ("mapStateDescriptor",String.class, TableProcessDwd.class);
        BroadcastStream<TableProcessDwd> broadcastDS = tpDS.broadcast(mapStateDescriptor);

//        关联主流数据和广播流的数据
        BroadcastConnectedStream<JSONObject, TableProcessDwd> connectDS = jsonObjDS.
                connect(broadcastDS);
//        对关联后的数据进行处理
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcessDwd>> splitDS = connectDS.process(new BaseDbTableProcessFunction(mapStateDescriptor));
//        写到kafka的不同主题中
        splitDS.print();
        splitDS.sinkTo(finksink.getKafkaSink());


    env.execute("DwdBaseDb");
    }

}
