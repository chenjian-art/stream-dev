package com.cj.realtime_Dim;
import com.cj.Base.BaseApp;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.cj.constat.constat;
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
import com.cj.bean.CommonTable;
import com.cj.realtime_Dim.flinkfcation.Tablepeocessfcation;
import com.cj.realtime_Dim.flinkfcation.flinksinkHbase;
import com.cj.realtime_Dim.flinkfcation.flinksorceutil;
import com.cj.utils.Hbaseutli;
/**
 * @Package com.cj.realtime_Dim.Dim_APP
 * @Author chen.jian
 * @Date 2025/5/5 10:48
 * @description: dim
 */
public class Dim_App extends BaseApp {

    public static void main(String[] args) throws Exception {
        new Dim_App().start(10001,1,"dim_app",constat.TOPIC_DB);
    }
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> kafkaDs = kafkaStrDS.process(new ProcessFunction<String, JSONObject>() {
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

        //cdc
        MySqlSource<String> getmysqlsource = flinksorceutil.getmysqlsource("gmall2025_config", "table_process_dim");
        DataStreamSource<String> mySQL_source = env.fromSource(getmysqlsource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(1);// 设置 sink 节点并行度为 1


        SingleOutputStreamOperator<CommonTable> tpds = mySQL_source.map(new MapFunction<String, CommonTable>() {
            @Override
            public CommonTable map(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                String op = jsonObject.getString("op");
                CommonTable commonTable = null;
                if ("d".equals(op)) {
                    commonTable = jsonObject.getObject("before", CommonTable.class);
                } else {
                    commonTable = jsonObject.getObject("after", CommonTable.class);
                }
                commonTable.setOp(op);
                return commonTable;
            }
        });

        tpds.print();
        tpds.map(
                new RichMapFunction<CommonTable, CommonTable>() {

                    private Connection hbaseconn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseconn = Hbaseutli.getHBaseConnection();

                    }

                    @Override
                    public void close() throws Exception {
                        Hbaseutli.closeHBaseConnection(hbaseconn);
                    }

                    @Override
                    public CommonTable map(CommonTable commonTable) throws Exception {
                        String op = commonTable.getOp();
                        //获取Hbase中维度表的表名
                        String sinkTable = commonTable.getSinkTable();
                        //获取在HBase中建表的列族
                        String[] sinkFamilies = commonTable.getSinkFamily().split(",");
                        if ("d".equals(op)) {
                            //从配置表中删除了一条数据  将hbase中对应的表删除掉
                            Hbaseutli.dropHBaseTable(hbaseconn, constat.HBASE_NAMESPACE, sinkTable);
                        } else if ("r".equals(op) || "c".equals(op)) {
                            //从配置表中读取了一条数据或者向配置表中添加了一条配置   在hbase中执行建表
                            Hbaseutli.createHBaseTable(hbaseconn, constat.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        }
                        else {
                            //对配置表中的配置信息进行了修改   先从hbase中将对应的表删除掉，再创建新表
                            Hbaseutli.dropHBaseTable(hbaseconn, constat.HBASE_NAMESPACE, sinkTable);
                            Hbaseutli.createHBaseTable(hbaseconn, constat.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        }
                        return commonTable;
                    }
                });
        MapStateDescriptor<String, CommonTable> tableMapStateDescriptor = new MapStateDescriptor<>
                ("maps", String.class, CommonTable.class);
        BroadcastStream<CommonTable> broadcast = tpds.broadcast(tableMapStateDescriptor);

        BroadcastConnectedStream<JSONObject, CommonTable> connects = kafkaDs.connect(broadcast);
        //处理流合并
        SingleOutputStreamOperator<Tuple2<JSONObject, CommonTable>> dimDS = connects.process(
                new Tablepeocessfcation(tableMapStateDescriptor)
        );

        dimDS.print();
        dimDS.addSink(new flinksinkHbase());

    }
}

