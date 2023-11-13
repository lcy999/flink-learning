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
public class TestCollectionForAggUdf {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        FlinkLocalRunHandler flinkLocalRunHandler = new FlinkLocalRunHandler() {

            @Override
            public List<GeneratorDataInfo> generateData() {

                GeneratorDataInfo dataInfo01 = buildGeneratorDataInfo("T02",
                        Lists.newArrayList("id", "item_emb", "text", "ts"), GeneratorDataTool.generateData01());


                return Lists.newArrayList(dataInfo01);
            }

            @Override
            public List<String> generateRunSql() {

                String sql1 = "CREATE TABLE upsertSink ("
                        + "  id INT,"
//                        + "  num DECIMAL(10,4),"
                        + "  num String,"
                        + "  ts TIMESTAMP(3)"
                        + ") WITH ("
                        + "  'connector'='print'"
                        + ")";

                String sql3="create function last_value_time as 'com.lcy.flinksql.udf.LastValueByTime'";
                String sql4="create function avg_string as 'com.lcy.flinksql.udf.AvgForSplitString'";

                String sql2 = "INSERT INTO upsertSink SELECT id " +
                        " , avg_string(item_emb) as num " +
                        " , max(ts) FROM T02 WHERE id IN (1,2) group by id";

                return Lists.newArrayList(sql1,sql3,sql4, sql2);
            }
        };

        flinkLocalRunHandler.executeSql();

    }




}
