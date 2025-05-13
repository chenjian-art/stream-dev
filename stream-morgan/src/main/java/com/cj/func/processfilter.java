package com.cj.func;

import com.alibaba.fastjson.JSONObject;
import com.mysql.cj.xdevapi.Schema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.cj.func.processfilter
 * @Author chen.jian
 * @Date 2025/5/13 下午4:51
 * @description:
 */
public class processfilter extends KeyedProcessFunction<Integer, JSONObject, JSONObject> {

    private ValueState<JSONObject> latestState;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<JSONObject> descriptor = new ValueStateDescriptor<>(
                "latestState",
                TypeInformation.of(JSONObject.class)
        );
        latestState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
        JSONObject json = new JSONObject();
        Long ts = value.getLong("ts");

        if (json == null || ts > json.getLong("ts")) {
            latestState.update(value);
            out.collect(value);
        }
    }
}
