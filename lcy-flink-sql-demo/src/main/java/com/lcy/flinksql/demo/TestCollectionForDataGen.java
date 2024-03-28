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
public class TestCollectionForDataGen {

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


                String sqlSource = "CREATE TABLE source_datagen ("
                        + "  id INT,"
                        + "  age INT"
                        + ") WITH ("
                        + "  'connector'='datagen',"
                        + "  'rows-per-second'='1',"
                        + "  'fields.age.max'='100',"
                        + "  'fields.age.min'='1',"
                        + "  'fields.id.kind'='sequence',"
                        + "  'fields.id.start'='1',"
                        + "  'fields.id.end'='100'"
                        + ")";

                String sqlSink = "CREATE TABLE upsertSink ("
                        + "  id INT,"
                        + "  age INT"
                        + ") WITH ("
                        + "  'connector'='print'"
                        + ")";


                String sqlInsert = "insert into upsertSink select id, age from source_datagen" ;

                return Lists.newArrayList(sqlSource, sqlSink, sqlInsert);
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

//                每条发一条，最多发10条
                String sqlSource = "CREATE TABLE source_datagen ("
                        + "  id INT,"
                        + "  age INT"
                        + ") WITH ("
                        + "  'connector'='datagen',"
                        + "  'rows-per-second'='1',"
                        + "  'number-of-rows'='10'"
                        + ")";

                String sqlSink = "CREATE TABLE upsertSink ("
                        + "  id INT,"
                        + "  age INT"
                        + ") WITH ("
                        + "  'connector'='print'"
                        + ")";


                String sqlInsert = "insert into upsertSink select id, age from source_datagen" ;

                return Lists.newArrayList(sqlSource, sqlSink, sqlInsert);
            }
        };

        flinkLocalRunHandler.executeSql();


    }




}
