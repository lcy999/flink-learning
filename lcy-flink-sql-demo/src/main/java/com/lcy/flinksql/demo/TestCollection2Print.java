package com.lcy.flinksql.demo;

import com.lcy.flinksql.utils.GeneratorDataTool;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.ExecutionException;

/**
 * @author: lcy
 * @date: 2023/7/27
 **/
public class TestCollection2Print {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.getConfig().setParallelism(1);

        EnvironmentSettings tableEnvSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, tableEnvSettings);

        GeneratorDataTool.GeneratorDataInfo dataInfo01 = GeneratorDataTool.buildGeneratorDataInfo(env, tEnv, "T01"
                , Lists.newArrayList("id", "num", "text", "ts"));
        GeneratorDataTool.generator4TupleDataStream01(dataInfo01);
        GeneratorDataTool.GeneratorDataInfo dataInfo02 = GeneratorDataTool.buildGeneratorDataInfo(env, tEnv, "T02"
                , Lists.newArrayList("id", "item_emb", "text", "ts"));
        GeneratorDataTool.generator4TupleDataStream02(dataInfo02);


        tEnv.executeSql(
                "CREATE TABLE upsertSink ("
                        + "  id INT,"
                        + "  num DECIMAL(10,4),"
                        + "  ts TIMESTAMP(3)"
                        + ") WITH ("
                        + "  'connector'='print'"
                        + ")");

        tEnv.executeSql("INSERT INTO upsertSink SELECT id " +
                " , CAST(avg(CAST(SPLIT_INDEX(item_emb,',',0) AS DECIMAL(10,4))) AS DECIMAL(10,4)) as item_emb" +
                " , max(ts) FROM T02 WHERE id IN (1,2) group by id")
                .await();

    }




}
