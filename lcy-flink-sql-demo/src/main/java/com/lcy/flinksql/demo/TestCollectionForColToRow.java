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
public class TestCollectionForColToRow {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        FlinkLocalRunHandler flinkLocalRunHandler = new FlinkLocalRunHandler() {

            @Override
            public List<GeneratorDataInfo> generateData() {

                GeneratorDataInfo dataInfo01 = buildGeneratorDataInfo("T01",
                        Lists.newArrayList("id", "itemEmb", "userInfo", "ts")
                        , GeneratorDataTool.generateData05());


                return Lists.newArrayList(dataInfo01);
            }

            @Override
            public List<String> generateRunSql() {

                String sql1 = "CREATE TABLE upsertSink ("
                        + "  id INT,"
                        + "  k STRING,"
                        + "  v STRING"
                        + ") WITH ("
                        + "  'connector'='print'"
                        + ")";

                String sql1_1="create view view_v1(id,arr) as\n" +
                        "select id,ARRAY[ARRAY['k1','v1'],ARRAY['k2','v2']] from T01";

                String sql2 = "INSERT INTO upsertSink\n" +
                        "select id,allUser[1],allUser[2]\n" +
                        "FROM view_v1,UNNEST(view_v1.arr) as T(allUser)";

                return Lists.newArrayList(sql1, sql1_1,sql2);
            }
        };

        flinkLocalRunHandler.executeSql();

    }




}
