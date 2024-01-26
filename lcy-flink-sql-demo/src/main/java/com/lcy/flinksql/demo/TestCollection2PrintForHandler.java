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
public class TestCollection2PrintForHandler {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        FlinkLocalRunHandler flinkLocalRunHandler = new FlinkLocalRunHandler() {

            @Override
            public List<GeneratorDataInfo> generateData() {

                GeneratorDataInfo dataInfo01 = buildGeneratorDataInfo("T02",
                        Lists.newArrayList("id", "item_emb", "text", "ts"), GeneratorDataTool.generateData01());

                GeneratorDataInfo dataInfo02 = buildGeneratorDataInfo("T01",
                        Lists.newArrayList("id", "num", "text", "ts"), GeneratorDataTool.generateData02());

                return Lists.newArrayList(dataInfo01, dataInfo02);
            }

            @Override
            public List<String> generateRunSql() {

                String sql1 = "CREATE TABLE upsertSink ("
                        + "  id INT,"
                        + "  num DECIMAL(10,4),"
                        + "  ts TIMESTAMP(3),"
                        + "  content STRING"
                        + ") WITH ("
                        + "  'connector'='print'"
                        + ")";

                String sql2 = "INSERT INTO upsertSink SELECT id " +
                        " , CAST(avg(CAST(SPLIT_INDEX(item_emb,',',0) AS DECIMAL(10,4))) AS DECIMAL(10,4)) as item_emb" +
                        " , max(ts) " +
                        ", distinct 'abc'" +
                        "FROM T02 WHERE id IN (1,2) group by id";

                return Lists.newArrayList(sql1, sql2);
            }
        };

        flinkLocalRunHandler.executeSql();

    }




}
