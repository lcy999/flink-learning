package com.lcy.flinksql.demo;

import com.lcy.flinksql.utils.FlinkLocalRunHandler;
import com.lcy.flinksql.utils.GeneratorDataTool;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.junit.After;
import org.junit.Test;

import java.util.List;

/**
 * @author: lcy
 * @date: 2023/7/27
 **/
public class TestCollectionForTestConvertTime {

    @Test
    public void t03(){
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
                        + "  age INT,"
                        + "  ts as PROCTIME()"
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
                        + "  age INT,"
                        + "  timeDiff BIGINT"
                        + ") WITH ("
                        + "  'connector'='print'"
                        + ")";


                String sqlInsert = "insert into upsertSink select id, age,TIMESTAMPDIFF(SECOND, ts, cast('2024-04-17 00:00:00' as TIMESTAMP)) from source_datagen" ;

                return Lists.newArrayList(sqlSource, sqlSink, sqlInsert);
            }
        };

        flinkLocalRunHandler.executeSql();
    }

    @After
    public void finish() throws InterruptedException {
        Thread.sleep(Integer.MAX_VALUE);
    }

    @Test
    public void t02(){
        FlinkLocalRunHandler flinkLocalRunHandler = new FlinkLocalRunHandler() {

            @Override
            public List<GeneratorDataInfo> generateData() {

                GeneratorDataInfo dataInfo01 = buildGeneratorDataInfo("T01",
                        Lists.newArrayList("id", "item_emb", "text", "ts"), GeneratorDataTool.generateData03());


                return Lists.newArrayList(dataInfo01);
            }

            @Override
            public List<String> generateRunSql() {

                String sql1 = "CREATE TABLE upsertSink ("
                        + "  id INT,"
                        + "  ts TIMESTAMP(3),"
                        + "  tsDate String,"
                        + "  tsNow String,"
                        + "  myDate DATE,"
                        + "  diff int"
                        + ") WITH ("
                        + "  'connector'='print'"
                        + ")";
//                TIMESTAMPDIFF(DAY,TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)),CURRENT_TIMESTAMP)
                String sql2 = "INSERT INTO upsertSink SELECT id " +
                        " , TO_TIMESTAMP(DATE_FORMAT(FROM_UNIXTIME(ts/1000),'yyyy-MM-dd HH:mm:ss'))" +
                        " , DATE_FORMAT(FROM_UNIXTIME(ts/1000), 'yyyy-MM-dd')" +
                        " , DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyyMMdd000000')" +
                        " , cast(text as Date)" +
//                        " , TIMESTAMPDIFF(DAY,TO_TIMESTAMP_LTZ(ts,3),CURRENT_TIMESTAMP) " +
                        " , TIMESTAMPDIFF(MINUTE,TO_TIMESTAMP('2024-01-16 09:42','yyyy-MM-dd HH:mm'),TO_TIMESTAMP('2024-01-16 09:43','yyyy-MM-dd HH:mm')) " +
                        " FROM T01 where '2023-11-15 16:20:00 596' < '2023-11-16 00:00:00' " +
                        "" +
                        "";

                return Lists.newArrayList(sql1, sql2);
            }
        };

        flinkLocalRunHandler.executeSql();

    }




}
