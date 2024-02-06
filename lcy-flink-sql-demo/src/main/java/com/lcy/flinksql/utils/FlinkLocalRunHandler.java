package com.lcy.flinksql.utils;

import com.lcy.flinksql.source.AutoCarSource;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author: lcy
 * @date: 2023/7/27
 **/
@Slf4j
public abstract class FlinkLocalRunHandler {

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    public FlinkLocalRunHandler(){
        this(null);
    }

    public FlinkLocalRunHandler(Configuration configuration){
        if (configuration == null) {
            env = StreamExecutionEnvironment.getExecutionEnvironment();
        }else{
            env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        }
        env.getConfig().enableObjectReuse();
        env.getConfig().setParallelism(1);

        EnvironmentSettings tableEnvSettings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        tEnv = StreamTableEnvironment.create(env, tableEnvSettings);

        configEnv(env, tEnv);
    }

    public FlinkLocalRunHandler(Configuration configuration, boolean isAutoCar){
        this(configuration);
        if(isAutoCar){
            DataStreamSource<Tuple4<Integer, Integer, Double, Long>> tuple4DataStreamSource = env.addSource(AutoCarSource.create(2));
            ArrayList<String> fieldNames = Lists.newArrayList("id", "speed", "distance", "time");
            List<Expression> expressions = fieldNames.stream().map(fname -> $(fname)).collect(Collectors.toList());
            Expression[] fieldNamesExpression = expressions.toArray(new Expression[fieldNames.size()]);
            tEnv.registerTable("autoCar" , tEnv.fromDataStream(tuple4DataStreamSource, fieldNamesExpression));

        }

    }

    public void configEnv(StreamExecutionEnvironment env,StreamTableEnvironment tEnv){

    }


    public abstract List<GeneratorDataInfo> generateData();

    public abstract List<String> generateRunSql();

    public void executeSql(){
        List<GeneratorDataInfo> generatorDataInfos = generateData();
        for (GeneratorDataInfo generatorDataInfo : generatorDataInfos) {
            registerTable(generatorDataInfo);
        }

        List<String> sqls = generateRunSql();
        StatementSet statementSet = tEnv.createStatementSet();
        for (String sql : sqls) {
            try {
                if(sql.startsWith("insert into")){
                    statementSet.addInsertSql(sql);
                }else{
                    tEnv.executeSql(sql);
                }

            }catch (SqlParserException sqlParserException){
                log.error("parse failed sql: "+sql);
                sqlParserException.printStackTrace();
            }
        }
        statementSet.execute();

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
