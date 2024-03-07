package com.lcy.flinksql.demo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author: lcy
 * @date: 2024/2/6
 **/
public class KafkaDorisDemoForMoreApplication {
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
        EnvironmentSettings tableEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, tableEnvSettings);
        String sourceSql=String.format("CREATE TABLE user_info (\n" +
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
                "  addr STRING,\n" +
                "  dt STRING,\n" +
                "  hr STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = '%s',\n" +
                "  'table.identifier' = '%s',\n" +
                "  'username' = '%s',\n" +
                "  'password' = '#wed34k_1L'\n" +
                ")", dorisUrl, dorisTable, dorisUser);

        System.out.println("sourceSql: "+sourceSql);
        System.out.println("sinkSql: "+sinkSql);

        String dml="insert into user_info_kafka2doris_01 select * from user_info";

        String sinkPrintSql="CREATE TABLE sink_print_01 (\n" +
                "  uname STRING,\n" +
                "  addr STRING,\n" +
                "  dt STRING,\n" +
                "  hr STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")";

        String dmlPrint="insert into sink_print_01 select * from user_info";

        StreamTableEnvironment tableEnv02 = StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment(), EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        tableEnv02.executeSql(sourceSql);
        tableEnv02.executeSql(sinkPrintSql.replace("_01","_02"));
        tableEnv02.executeSql(dmlPrint.replace("_01","_02"));

        tableEnv.executeSql(sourceSql);
        tableEnv.executeSql(sinkSql);
        tableEnv.executeSql(dml);





        StreamTableEnvironment tableEnv03 = StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment(), EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        tableEnv03.executeSql(sourceSql);
        tableEnv03.executeSql(sinkPrintSql.replace("_01","_03"));
        tableEnv03.executeSql(dmlPrint.replace("_01","_03"));

        StreamTableEnvironment tableEnv04 = StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment(), EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        tableEnv04.executeSql(sourceSql);
        tableEnv04.executeSql(sinkPrintSql.replace("_01","_04"));
        tableEnv04.executeSql(dmlPrint.replace("_01","_04"));

        StreamTableEnvironment tableEnv05 = StreamTableEnvironment.create(StreamExecutionEnvironment.getExecutionEnvironment(), EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build());
        tableEnv05.executeSql(sourceSql);
        tableEnv05.executeSql(sinkPrintSql.replace("_01","_05"));
        tableEnv05.executeSql(dmlPrint.replace("_01","_05"));

    }
}
