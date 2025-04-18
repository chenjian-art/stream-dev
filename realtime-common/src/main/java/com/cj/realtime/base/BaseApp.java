package com.cj.realtime.base;

import com.cj.realtime.constant.Constant;
import com.cj.realtime.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @Package com.cj.realtime.base.BaseApp
 * @Author chen.jian
 * @Date 2025/4/9 21:51
 * @description: 4
 */
public abstract class BaseApp {
    public void start(int port, int parallelism, String ckAndGroupId, String topic) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port",port);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        //开启检查点  5秒一次 精准写入
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
//        设置了检查点的超时时间为 60000 毫秒（即 60 秒）。如果在 60 秒内检查点操作没有完成，就会被视为失败。
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
//        当作业被取消时，检查点数据不会被删除，而是会保留下来，这样在后续需要时可以利用这些检查点数据进行恢复操作。
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        两次检查点操作之间的最小间隔时间为 2000 毫秒（即 2 秒）。这是为了避免在短时间内频繁进行检查点操作，从而影响作业的正常处理性能。
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
//        表示在 30 天内允许的最大失败次数为 3 次。
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
//        状态后端用于管理 Flink 作业的状态数据，HashMapStateBackend 会将状态数据存储在 TaskManager 的内存中，适用于小规模的状态管理。
        env.setStateBackend(new HashMapStateBackend());
        //检查点储存位置
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://cdh01:8020/ck" + ckAndGroupId);
        System.setProperty("HADOOP_USER_NAME","root");


        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(topic, ckAndGroupId);

        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        handle(env,kafkaStrDS);

        env.execute();
    }

    public abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS);
}
