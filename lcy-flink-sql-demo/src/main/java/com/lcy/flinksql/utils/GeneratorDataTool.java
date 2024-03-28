package com.lcy.flinksql.utils;

import lombok.Builder;
import lombok.Data;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.expressions.Expression;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author: lcy
 * @date: 2023/7/27
 **/
public class GeneratorDataTool {


    public static void generatorDataStream(GeneratorDataInfo dataInfo, List<?> data) {
        dataInfo.setData(data);
        generatorDataStream(dataInfo);
    }

    public static List<?> generateData02(){
        List<Tuple4<Integer, Long, String, Timestamp>> data = new ArrayList<>();
        data.add(new Tuple4<>(1, 1L, "Hi", Timestamp.valueOf("1970-01-01 00:00:00.001")));
        data.add(new Tuple4<>(2, 2L, "Hello", Timestamp.valueOf("1970-01-01 00:00:00.002")));
        data.add(new Tuple4<>(3, 2L, "Hello world", Timestamp.valueOf("1970-01-01 00:00:00.003")));
        data.add(new Tuple4<>(4, 3L, "Hello world, how are you?", Timestamp.valueOf("1970-01-01 00:00:00.004")));
        data.add(new Tuple4<>(5, 3L, "I am fine.", Timestamp.valueOf("1970-01-01 00:00:00.005")));
        data.add(new Tuple4<>(6, 3L, "Luke Skywalker", Timestamp.valueOf("1970-01-01 00:00:00.006")));
        data.add(new Tuple4<>(7, 4L, "Comment#1", Timestamp.valueOf("1970-01-01 00:00:00.007")));
        data.add(new Tuple4<>(8, 4L, "Comment#2", Timestamp.valueOf("1970-01-01 00:00:00.008")));
        data.add(new Tuple4<>(9, 4L, "Comment#3", Timestamp.valueOf("1970-01-01 00:00:00.009")));
        data.add(new Tuple4<>(10, 4L, "Comment#4", Timestamp.valueOf("1970-01-01 00:00:00.010")));
        data.add(new Tuple4<>(11, 5L, "Comment#5", Timestamp.valueOf("1970-01-01 00:00:00.011")));
        data.add(new Tuple4<>(12, 5L, "Comment#6", Timestamp.valueOf("1970-01-01 00:00:00.012")));
        data.add(new Tuple4<>(13, 5L, "Comment#7", Timestamp.valueOf("1970-01-01 00:00:00.013")));
        data.add(new Tuple4<>(14, 5L, "Comment#8", Timestamp.valueOf("1970-01-01 00:00:00.014")));
        data.add(new Tuple4<>(15, 5L, "Comment#9", Timestamp.valueOf("1970-01-01 00:00:00.015")));
        data.add(new Tuple4<>(16, 6L, "Comment#10", Timestamp.valueOf("1970-01-01 00:00:00.016")));
        data.add(new Tuple4<>(17, 6L, "Comment#11", Timestamp.valueOf("1970-01-01 00:00:00.017")));
        data.add(new Tuple4<>(18, 6L, "Comment#12", Timestamp.valueOf("1970-01-01 00:00:00.018")));
        data.add(new Tuple4<>(19, 6L, "Comment#13", Timestamp.valueOf("1970-01-01 00:00:00.019")));
        data.add(new Tuple4<>(20, 6L, "Comment#14", Timestamp.valueOf("1970-01-01 00:00:00.020")));
        data.add(new Tuple4<>(21, 6L, "Comment#15", Timestamp.valueOf("1970-01-01 00:00:00.021")));

        return data;
    }

    public static List<?> generateData01(){
        List<Tuple4<Integer, String, String, Timestamp>> data = new ArrayList<>();
        data.add(new Tuple4<>(1, "-0.5486", "Hi", Timestamp.valueOf("1970-01-02 00:00:00.001")));
        data.add(new Tuple4<>(1, "-0.5473", "Hello", Timestamp.valueOf("1970-01-01 00:00:00.002")));
        data.add(new Tuple4<>(2, "-0.0574", "Hello world", Timestamp.valueOf("1970-01-01 00:00:00.003")));
        data.add(new Tuple4<>(2, "-0.0679", "Hello world, how are you?", Timestamp.valueOf("1970-01-01 00:00:00.004")));
        data.add(new Tuple4<>(5, "3L", "I am fine.", Timestamp.valueOf("1970-01-01 00:00:00.005")));
        data.add(new Tuple4<>(6, "3L", "Luke Skywalker", Timestamp.valueOf("1970-01-01 00:00:00.006")));
        data.add(new Tuple4<>(7, "4L", "Comment#1", Timestamp.valueOf("1970-01-01 00:00:00.007")));
        data.add(new Tuple4<>(8, "4L", "Comment#2", Timestamp.valueOf("1970-01-01 00:00:00.008")));

        return data;
    }

    public static List<?> generateData03(){
        List<Tuple4<Integer, String, String, Long>> data = new ArrayList<>();
        data.add(new Tuple4<>(1, "-0.5486", "2024-03-22 15:05:05", 1692932622184L));
        data.add(new Tuple4<>(1, "-0.5473", "2024-03-22 16:05:05", 1703903982000L));

        return data;
    }

    public static List<?> generateData04(){
        List<Tuple4<Integer, String, String, Long>> data = new ArrayList<>();
        data.add(new Tuple4<>(1, "-0.5486", "[{\"uname\":\"lcy61\",\"addr\":\"shanghai\",\"dt\":\"2023-09-19\",\"hr\":\"23\"},{\"uname\":\"lcy52\",\"addr\":\"shenzhen\",\"dt\":\"2023-09-19\",\"hr\":\"23\"},{\"uname\":\"lcy53\",\"addr\":\"beijing\",\"dt\":\"2023-09-19\",\"hr\":\"23\"}]", 1697593647540L));
        return data;
    }

    public static List<?> generateData05(){
        List<Tuple4<Integer, String, String, Long>> data = new ArrayList<>();
        data.add(new Tuple4<>(1, "-0.5486", "lcy61:shanghai:2023-09-19,lcy62:shanghai02:2023-10-19,lcy63:shanghai03:2023-11-19", 1697593647540L));

        return data;
    }


    public static void generatorDataStream(GeneratorDataInfo dataInfo) {

        List<?> data = dataInfo.getData();
        List<Expression> fieldNames = dataInfo.getFieldNames();
        StreamExecutionEnvironment env = dataInfo.getEnv();
        StreamTableEnvironment tEnv = dataInfo.getTEnv();
        String tableName = dataInfo.getTableName();

        Collections.shuffle(data);
        Expression[] fieldNamesExpression = fieldNames.toArray(new Expression[fieldNames.size()]);
        Table t02 = tEnv.fromDataStream(env.fromCollection(data), fieldNamesExpression);
        tEnv.registerTable(tableName, t02);
    }

    public static void generator4TupleDataStream02(GeneratorDataInfo dataInfo) {
        generatorDataStream(dataInfo, generateData01());
    }

    public static void generator4TupleDataStream01(GeneratorDataInfo dataInfo) {
        generatorDataStream(dataInfo, generateData02());
    }

    public static GeneratorDataInfo buildGeneratorDataInfo(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, String tableName, List<String> fieldNames){

        List<Expression> expressions = fieldNames.stream().map(fname -> $(fname)).collect(Collectors.toList());

        return GeneratorDataInfo.builder()
                .env(env)
                .tEnv(tEnv)
                .tableName(tableName)
                .fieldNames(expressions)
                .build();
    }

    @Data
    @Builder
    public static class GeneratorDataInfo {
        private StreamExecutionEnvironment env;
        private StreamTableEnvironment tEnv;
        private String tableName;
        private List<?> data;
        private List<Expression> fieldNames;
    }

}
