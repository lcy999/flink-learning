package com.lcy.flinksql.demo;

import com.lcy.flinksql.utils.FlinkLocalRunHandler;
import com.lcy.flinksql.utils.GeneratorDataTool;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author: lcy
 * @date: 2023/7/27
 **/
public class TestCollectionForTestConvertTime {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
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
                        + "  diff int"
                        + ") WITH ("
                        + "  'connector'='print'"
                        + ")";
//                TIMESTAMPDIFF(DAY,TO_TIMESTAMP(FROM_UNIXTIME(ts/1000)),CURRENT_TIMESTAMP)
                String sql2 = "INSERT INTO upsertSink SELECT id " +
                        " , TO_TIMESTAMP(DATE_FORMAT(FROM_UNIXTIME(ts/1000),'yyyy-MM-dd HH:mm:ss'))" +
                        " , DATE_FORMAT(FROM_UNIXTIME(ts/1000), 'yyyy-MM-dd')" +
                        " , DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyyMMdd000000')" +
                        " , 999 " +
                        " FROM T01 where '2023-11-15 16:20:00 596' < '2023-11-16 00:00:00' and 100 % 10=0";

                return Lists.newArrayList(sql1, sql2);
            }
        };

        flinkLocalRunHandler.executeSql();

    }




}
