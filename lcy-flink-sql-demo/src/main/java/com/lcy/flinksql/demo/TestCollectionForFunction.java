package com.lcy.flinksql.demo;

import com.lcy.flinksql.utils.FlinkLocalRunHandler;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author: lcy
 * @date: 2023/7/27
 **/
public class TestCollectionForFunction {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration conf = new Configuration();

        FlinkLocalRunHandler flinkLocalRunHandler = new FlinkLocalRunHandler(conf, true) {

            @Override
            public List<GeneratorDataInfo> generateData() {

                return Lists.newArrayList();
            }

            @Override
            public List<String> generateRunSql() {


                String sql1 = "CREATE TABLE upsertSink ("
                        + "  id INT"
                        + "  ,mod_num INT"
                        + ") WITH ("
                        + "  'connector'='print'"
                        + ")";

                String sql2 = "insert into upsertSink select id,mod(120,39) from autoCar where mod(120,39)=3";

                return Lists.newArrayList(sql1, sql2);
            }
        };

        flinkLocalRunHandler.executeSql();

    }




}
