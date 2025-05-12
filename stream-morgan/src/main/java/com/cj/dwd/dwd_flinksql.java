package com.cj.dwd;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.cj.dwd.dwd_flinksql
 * @Author chen.jian
 * @Date 2025/5/12 19:34
 * @description:
 */
public class dwd_flinksql {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        tenv.executeSql("CREATE TABLE db (\n" +
                "  before MAP<string,string>,\n" +
                "  after Map<String,String>,\n" +
                "  source  Map<String,String>,\n" +
                "  op  String,\n" +
                "  ts_ms  bigint,\n" +
                "  proc_time  AS proctime()\n "+
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_db',\n" +
                "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");

        Table table = tenv.sqlQuery("select after['id'] as id," +
                "after['name'] as name," +
                "DATE_ADD('0000-01-01', INTERVAL after['birthday'] DAY) as birthday," +
                "after['gender'] as gender " +
                " from db where source['table'] = 'user_info'");
        tenv.toChangelogStream(table).print();



        env.execute();
    }
}
