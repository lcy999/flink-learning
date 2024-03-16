package com.lcy.flinksql.demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: lcy
 * @date: 2024/2/6
 **/
public class KafkaDorisDemoForStatementSet {
    public static void main(String[] args) {



        String kafkaServerUrl=args[0];
        String kafkaZKUrl=args[1];
        String kafkaTopic=args[2];
        String kafkaScanMode=args[3];
        String kafkaUser=args[4];
        String kafkaPwd=args[5];
        String dorisUrl=args[6];
        String dorisTable=args[7];
        String dorisUser=args[8];

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings tableEnvSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, tableEnvSettings);
        String sourceSql=String.format("CREATE TABLE user_info_01 (\n" +
                "  uname STRING,\n" +
                "  addr STRING,\n" +
                "  dt STRING,\n" +
                "  hr STRING\n" +
                ")  WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '%s',\n" +
                "  'properties.group.id' = 'cs_poc_user_info_51',\n" +
                "  'scan.startup.mode' = '%s',\n" +
                "  'format' = 'json',\n" +
                "  'properties.bootstrap.servers' = '%s',\n" +
                "  'properties.zookeeper.connect' = '%s',\n" +
                "  'properties.sasl.mechanism' = 'PLAIN',\n" +
                "  'properties.security.protocol' = 'SASL_PLAINTEXT',\n" +
                "  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";'\n" +
                ")",kafkaTopic, kafkaScanMode, kafkaServerUrl, kafkaZKUrl,kafkaUser,kafkaPwd);


        String sinkSql=String.format("CREATE TABLE user_info_kafka2doris_01 (\n" +
                "  uname STRING,\n" +
                "  count_num int\n" +
                ") WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = '%s',\n" +
                "  'table.identifier' = '%s',\n" +
                "  'username' = '%s',\n" +
                "  'password' = '#wed34k_1L'\n" +
                ")", dorisUrl, dorisTable, dorisUser);

        System.out.println("sourceSql: "+sourceSql);
        System.out.println("sinkSql: "+sinkSql);

        String filterView="create view view_user_filter as select * from user_info where addr='shanghai'";

        String dml="insert into user_info_kafka2doris_01 select uname,count(1) from view_user_filter group by uname";

        StatementSet statementSet = tableEnv.createStatementSet();

        tableEnv.executeSql(sourceSql);
        tableEnv.executeSql(filterView);
        tableEnv.executeSql(sinkSql);
        statementSet.addInsertSql(dml);


        String sinkPrintSql="CREATE TABLE sink_print (\n" +
                "  addr STRING,\n" +
                "  count_num int\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";

        String dmlPrint="insert into sink_print select addr,count(1) from view_user_filter group by addr";

        tableEnv.executeSql(sinkPrintSql);
        statementSet.addInsertSql(dmlPrint);

        statementSet.execute();

    }
}
