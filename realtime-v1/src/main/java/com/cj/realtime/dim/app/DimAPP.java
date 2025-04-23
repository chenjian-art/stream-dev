package com.cj.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.cj.realtime.base.BaseApp;
import com.cj.realtime.bean.TableProcessDim;
import com.cj.realtime.constant.Constant;
import com.cj.realtime.dim.function.HBaseSinkFunction;
import com.cj.realtime.dim.function.TableProcessFunction;
import com.cj.realtime.util.FlinkSourceUtil;
import com.cj.realtime.util.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Package com.cj.realtime.dim.app.DimAPP
 * @Author chen.jian
 * @Date 2025/4/8 9:27
 * @description: 2
 */
public class DimAPP extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DimAPP().start(10001,4,"dim_app",Constant.TOPIC_DB);

    }


    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> jsonObjDS = etl(kafkaStrDS);

        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("gmall2025_config", "table_process_dim");
        DataStreamSource<String> mySQLSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");


        SingleOutputStreamOperator<TableProcessDim> tpDS = mySQLSource.map(new MapFunction<String, TableProcessDim>() {
            @Override
            public TableProcessDim map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                String op = jsonObject.getString("op");
                TableProcessDim commonTable = null;
                if ("d".equals(op)) {
                    commonTable = jsonObject.getObject("before", TableProcessDim.class);
                } else {
                    commonTable = jsonObject.getObject("after", TableProcessDim.class);
                }
                commonTable.setOp(op);
                return commonTable;
            }
        });
//        TableProcessDim(sourceTable=user_info, sinkTable=dim_user_info, sinkColumns=id,login_name,name,user_level,birthday,gender,create_time,operate_time, sinkFamily=info, sinkRowKey=id, op=c)
//        TableProcessDim(sourceTable=spu_info, sinkTable=dim_spu_info, sinkColumns=id,spu_name,description,category3_id,tm_id, sinkFamily=info, sinkRowKey=id, op=c)
//        TableProcessDim(sourceTable=sku_info, sinkTable=dim_sku_info, sinkColumns=id,spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time, sinkFamily=info, sinkRowKey=id, op=c)
//        tpDS.print();


        tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {

                    private Connection hbaseconn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseconn=HBaseUtil.getHBaseConnection();

                    }
                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseconn);
                    }
                    @Override
                    public TableProcessDim map(TableProcessDim commonTable) throws Exception {
                        String op = commonTable.getOp();
                        //获取Hbase中维度表的表名
                        String sinkTable = commonTable.getSinkTable();
                        //获取在HBase中建表的列族
                        String[] sinkFamilies = commonTable.getSinkFamily().split(",");
                        if ("d".equals(op)) {
                            //从配置表中删除了一条数据  将hbase中对应的表删除掉
                            HBaseUtil.dropHBaseTable(hbaseconn,Constant.HBASE_NAMESPACE,sinkTable);
                        } else if ("r".equals(op) || "c".equals(op)) {
                            //从配置表中读取了一条数据或者向配置表中添加了一条配置   在hbase中执行建表
                            HBaseUtil.createHBaseTable(hbaseconn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        } else {
                            //对配置表中的配置信息进行了修改   先从hbase中将对应的表删除掉，再创建新表
                            HBaseUtil.dropHBaseTable(hbaseconn,Constant.HBASE_NAMESPACE,sinkTable);
                            HBaseUtil.createHBaseTable(hbaseconn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }
                        return commonTable;
                    }
                });

//        TableProcessDim(sourceTable=sku_info, sinkTable=dim_sku_info, sinkColumns=id,spu_id,price,sku_name,sku_desc,weight,tm_id,category3_id,sku_default_img,is_sale,create_time, sinkFamily=info, sinkRowKey=id, op=c)
//        TableProcessDim(sourceTable=spu_info, sinkTable=dim_spu_info, sinkColumns=id,spu_name,description,category3_id,tm_id, sinkFamily=info, sinkRowKey=id, op=c)
//        TableProcessDim(sourceTable=user_info, sinkTable=dim_user_info, sinkColumns=id,login_name,name,user_level,birthday,gender,create_time,operate_time, sinkFamily=info, sinkRowKey=id, op=c)
//        tpDS.print();

        MapStateDescriptor<String, TableProcessDim> MapStateDescriptor = new MapStateDescriptor<>
                ("maps", String.class, TableProcessDim.class);

        BroadcastStream<TableProcessDim> broadcastOS = tpDS.broadcast(MapStateDescriptor);

        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastOS);

        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDim>> dimDS = connectDS.process(
                new TableProcessFunction(MapStateDescriptor)
        );
        dimDS.print();


        dimDS.addSink(new HBaseSinkFunction());
    }

    private static SingleOutputStreamOperator<JSONObject> etl(DataStreamSource<String> kafkaStrDS){
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                JSONObject jsonObj = JSON.parseObject(s);
                String db = jsonObj.getJSONObject("source").getString("db");
                String type = jsonObj.getString("op");
                String data = jsonObj.getString("after");
                if ("gmall_config".equals(db)
                        && ("c".equals(type)
                        || "u".equals(type)
                        || "d".equals(type)
                        || "r".equals(type))
                        && data != null
                        && data.length() > 2
                ) {
                    collector.collect(jsonObj);
                }
            }
        });
        return jsonObjDS;
    }
}
