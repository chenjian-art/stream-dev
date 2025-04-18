package com.cj.realtime;

import lombok.SneakyThrows;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.doris.flink.source.DorisSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.List;
import java.util.Properties;

/**
 * @Package com.cj.realtime.Test01_Doris_SQL
 * @Author chen.jian
 * @Date 2025/4/17 15:42
 * @description: SSS
 */
public class Test02_Doris_API {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 2.检查点相关的设置
        env.enableCheckpointing(5000L);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));

        DorisOptions.Builder builder = DorisOptions.builder()
                .setFenodes("cdh03:8110")
                .setTableIdentifier("test_db.table1")
                .setUsername("root")
                .setPassword("123456");

        DorisSource<List<?>> dorisSource = DorisSource.<List<?>>builder()
                .setDorisOptions(builder.build())
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDeserializer(new SimpleListDeserializationSchema())
                .build();

        DataStreamSource<List<?>> stream1 = env.fromSource(dorisSource, WatermarkStrategy.noWatermarks(), "doris source");
//        stream1.print();


        DataStreamSource<String> source = env
                .fromElements(
                        "{\"siteid\": \"550\", \"citycode\": \"1001\", \"username\": \"ww\",\"pv\": \"100\"}");
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据

        DorisSink<String> sink = DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(DorisOptions.builder() // 设置 doris 的连接参数
                        .setFenodes("cdh03:8110")
                        .setTableIdentifier("test_db.table1")
                        .setUsername("root")
                        .setPassword("123456")
                        .build())
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
                        //.setLabelPrefix("doris-label")  // stream-load 导入的时候的 label 前缀
                        .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .setDeletable(false)
                        .setBufferCount(3) // 用于缓存stream load数据的缓冲条数: 默认 3
                        .setBufferSize(1024*1024) //用于缓存stream load数据的缓冲区大小: 默认 1M
                        .setMaxRetries(3)
                        .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,根据需要改成 json
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
        source.sinkTo(sink);

        env.execute();
    }
}