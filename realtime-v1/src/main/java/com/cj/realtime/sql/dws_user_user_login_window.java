package com.cj.realtime.sql;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.cj.realtime.sql.dws_user_user_login_window
 * @Author chen.jian
 * @Date 2025/4/16 15:35
 * @description: jjj
 */
public class dws_user_user_login_window {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.executeSql("CREATE TABLE KafkaTable (\n" +
                "  stt string,\n" +
                "  edt string,\n" +
                "  cur_date date,\n" +
                "  back_ct bigint,\n" +
                "  uu_ct bigint \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'dws_user_user_login_window',\n" +
                "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
        Table table = tenv.sqlQuery("select * from KafkaTable");
        tenv.toChangelogStream(table).print();


        env.execute();
    }
}
