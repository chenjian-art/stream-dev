package com.cj.realtime_Dwd;

import com.cj.constat.constat;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import com.cj.utils.Sqlutil;

/**
 * @Package com.cj.realtime_Dwd.DwdInteractionCommentInfo
 * @Author chen.jian
 * @Date 2025/4/10 19:02
 * @description: 评论事实表
 */

public class DwdInteractionCommentInfo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
//      flink sql读取kafka
        tenv.executeSql("" +
                "CREATE TABLE db (\n" +
                "  after Map<String,String>,\n" +
                "  source  Map<String,String>,\n" +
                "  op  String,\n" +
                "  ts_ms  bigint,\n" +
                "  before MAP<string,string> ,\n" +
                "  proc_time  AS proctime()\n "+
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic_db',\n" +
                "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json'\n" +
                ")");
//

        Table table = tenv.sqlQuery("select * from db ");
//        tenv.toChangelogStream(table).print();

        Table table1 = tenv.sqlQuery("select " +
                "after['id'] as id," +
                "after['user_id'] as user_id," +
                "after['sku_id'] as sku_id," +
                "after['appraise'] as appraise," +
                "after['comment_txt'] as comment_txt," +
                "ts_ms as ts," +
                "proc_time " +
                "from db where source['table'] = 'comment_info' ");
//        tenv.toChangelogStream(table1).print();

//      将表对象注册到表执行环境中
        tenv.createTemporaryView("comment_info",table1);
//        从HBase中读取字典数据 创建动态表
        tenv.executeSql("CREATE TABLE hbase (\n" +
                " dic_code String,\n" +
                " info ROW<dic_name String>,\n" +
                " PRIMARY KEY (dic_code) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'gmall_config:dim_base_dic',\n" +
                " 'zookeeper.quorum' = 'cdh01,cdh02,cdh03:2181'\n" +
                ");");

        Table table2 = tenv.sqlQuery("select * from hbase");
//        tenv.toChangelogStream(table2).print();

//        将评论表和字典表进行关联
        Table table3 = tenv.sqlQuery("SELECT  " +
                " id,user_id,sku_id,appraise,dic.dic_name,comment_txt,ts \n" +
                "FROM comment_info AS c \n" +
                "  left join hbase as dic \n" +
                "    ON c.appraise = dic.dic_code where id is not null");
        table3.execute().print();

//        将关联的结果写到kafka主题中
        tenv.executeSql("CREATE TABLE dwd_interaction_comment_info (\n" +
                "    id string,\n" +
                "    user_id string,\n" +
                "    sku_id string,\n" +
                "    appraise string,\n" +
                "    appraise_name string,\n" +
                "    comment_txt string,\n" +
                "    ts bigint,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") " +
                " WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'dwd_interaction_comment_info',\n" +
                "  'properties.bootstrap.servers' = 'cdh02:9092',\n" +
                "  'key.format' = 'json',\n" +
                "  'value.format' = 'json'\n" +
                ")");
        table3.executeInsert("dwd_interaction_comment_info");

        env.execute();
    }

}
