package com.cj.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.cj.realtime.bean.TableProcessDim;
import com.cj.realtime.constant.Constant;
import com.cj.realtime.util.HBaseUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Package com.cj.realtime.dim.function.HBaseSinkFunction
 * @Author chen.jian
 * @Date 2025/4/9 21:37
 * @description: 3
 */
public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcessDim>> {
    private Connection hbase_conn;
    @Override
    public void open(Configuration parameters) throws Exception {
        hbase_conn= HBaseUtil.getHBaseConnection();
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void invoke(Tuple2<JSONObject, TableProcessDim> value, Context context) throws Exception {
        JSONObject f0 = value.f0;
        TableProcessDim f1 = value.f1;
        String op = f0.getString("op");
        f0.remove("op");
        String sinkTable = f1.getSinkTable();
        String string = f0.getString(f1.getSinkRowKey());

        if ("d".equals(op))
        {
            HBaseUtil.delRow(hbase_conn, Constant.HBASE_NAMESPACE,sinkTable,string);
        }
        else {
            String sinkFamily = f1.getSinkFamily();
            HBaseUtil.putRow(hbase_conn,Constant.HBASE_NAMESPACE,sinkTable,string,sinkFamily,f0);


        }
    }
}
