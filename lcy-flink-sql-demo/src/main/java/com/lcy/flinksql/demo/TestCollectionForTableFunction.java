package com.lcy.flinksql.demo;

import com.lcy.flinksql.utils.FlinkLocalRunHandler;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.junit.After;
import org.junit.Test;

import java.util.List;

/**
 * @author: lcy
 * @date: 2023/7/27
 **/
public class TestCollectionForTableFunction {

    @Test
    public void t02(){
        Configuration conf = new Configuration();

        FlinkLocalRunHandler flinkLocalRunHandler = new FlinkLocalRunHandler(conf, true) {

            @Override
            public List<GeneratorDataInfo> generateData() {

                return Lists.newArrayList();
            }

            @Override
            public List<String> generateRunSql() {


                String sqlSink = "CREATE TABLE upsertSink ("
                        + "  id INT,"
                        + "  uid INT,"
                        + "  uname STRING"
                        + ") WITH ("
                        + "  'connector'='print'"
                        + ")";

                String sqlFunction="create function user_profile_table_func as 'com.lcy.flinksql.udf.UserRowTableFunction'";

                String sqlInsert = "insert into upsertSink select id, user_id,user_name from autoCar" +
                        ",LATERAL TABLE(user_profile_table_func(id)) t(user_id,user_name)";

                return Lists.newArrayList(sqlSink,sqlFunction, sqlInsert);
            }
        };

        flinkLocalRunHandler.executeSql();


    }


    @After
    public void after(){
        try {
            Thread.sleep(Integer.MAX_VALUE);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void t01(){
        Configuration conf = new Configuration();

        FlinkLocalRunHandler flinkLocalRunHandler = new FlinkLocalRunHandler(conf, true) {

            @Override
            public List<GeneratorDataInfo> generateData() {

                return Lists.newArrayList();
            }

            @Override
            public List<String> generateRunSql() {


                String sqlSink = "CREATE TABLE upsertSink ("
                        + "  id INT,"
                        + "  age INT"
                        + ") WITH ("
                        + "  'connector'='print'"
                        + ")";

                String sqlFunction="create function user_profile_table_func as 'com.lcy.flinksql.udf.UserProfileTableFunction'";

                String sqlInsert = "insert into upsertSink select id, age from autoCar" +
                        ",LATERAL TABLE(user_profile_table_func(id)) t(age)";

                return Lists.newArrayList(sqlSink,sqlFunction, sqlInsert);
            }
        };

        flinkLocalRunHandler.executeSql();


    }




}
