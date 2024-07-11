package com.lcy.flinksql.main;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author: lcy
 * @date: 2024/2/6
 **/
public class autoWriteHiveDemo {
    public static void main(String[] args) {



        String hiveDbName=args[0];
        String hiveConfPath=args[1];
        String checkpointPath=args[2];

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointStorage(checkpointPath);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        EnvironmentSettings tableEnvSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, tableEnvSettings);

        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(60));

        String hiveCatalog=String.format("CREATE CATALOG myhive WITH (\n" +
                "    'type' = 'hive',\n" +
                "    'hive-conf-dir' = '%s'\n" +
                ")",hiveConfPath);


        String sourceSql=String.format("create table source_user_01(\n" +
                "  uid bigint,\n" +
                "  uname string,\n" +
                "  uage int,\n" +
                "  updated_at timestamp(3)\n" +
                ")WITH (\n" +
                "  'connector'='datagen',\n" +
                "  'rows-per-second'='3',\n" +
                "  'fields.uage.max'='100',\n" +
                "  'fields.uage.min'='1',\n" +
                "  'fields.uid.min'='1',\n" +
                "  'fields.uid.max'='888888'\n" +
                ")");

        String dml= String.format("insert into myhive.%s select * from source_user_01", hiveDbName);

        System.out.println("sourceSql: "+sourceSql);
        System.out.println("hiveCatalog: "+hiveCatalog);
        System.out.println("dml: "+dml);




        tableEnv.executeSql(hiveCatalog);
        tableEnv.executeSql(sourceSql);
        tableEnv.executeSql(dml);


    }
}
