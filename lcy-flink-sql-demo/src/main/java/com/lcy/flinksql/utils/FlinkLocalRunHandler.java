package com.lcy.flinksql.utils;

import lombok.Builder;
import lombok.Data;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author: lcy
 * @date: 2023/7/27
 **/
public abstract class FlinkLocalRunHandler {

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    public FlinkLocalRunHandler(){
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();
        env.getConfig().setParallelism(1);

        EnvironmentSettings tableEnvSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        tEnv = StreamTableEnvironment.create(env, tableEnvSettings);
    }


    public abstract List<GeneratorDataInfo> generateData();

    public abstract List<String> generateRunSql();

    public void executeSql(){
        List<GeneratorDataInfo> generatorDataInfos = generateData();
        for (GeneratorDataInfo generatorDataInfo : generatorDataInfos) {
            registerTable(generatorDataInfo);
        }

        List<String> sqls = generateRunSql();
        for (String sql : sqls) {
            tEnv.executeSql(sql);
        }

    }

    private void registerTable(GeneratorDataInfo dataInfo) {

        List<?> data = dataInfo.getData();
        List<Expression> fieldNames = dataInfo.getFieldNames();
        String tableName = dataInfo.getTableName();

        Collections.shuffle(data);
        Expression[] fieldNamesExpression = fieldNames.toArray(new Expression[fieldNames.size()]);
        Table table = tEnv.fromDataStream(env.fromCollection(data), fieldNamesExpression);
        tEnv.registerTable(tableName, table);
    }

    protected GeneratorDataInfo buildGeneratorDataInfo(String tableName, List<String> fieldNames, List<?> data){

        List<Expression> expressions = fieldNames.stream().map(fname -> $(fname)).collect(Collectors.toList());
        return GeneratorDataInfo.builder()
                .tableName(tableName)
                .fieldNames(expressions)
                .data(data)
                .build();
    }

    @Data
    @Builder
    public static class GeneratorDataInfo {
        private String tableName;
        private List<?> data;
        private List<Expression> fieldNames;
        
    }


}
