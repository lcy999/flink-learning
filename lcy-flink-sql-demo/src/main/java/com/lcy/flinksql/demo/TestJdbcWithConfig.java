package com.lcy.flinksql.demo;

import com.lcy.flinksql.utils.FlinkLocalRunHandler;
import com.lcy.flinksql.utils.GeneratorDataTool;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author: lcy
 * @date: 2023/7/27
 **/
public class TestJdbcWithConfig {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        FlinkLocalRunHandler flinkLocalRunHandler = new FlinkLocalRunHandler() {

            @Override
            public void configEnv(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

            }

            @Override
            public List<GeneratorDataInfo> generateData() {

                GeneratorDataInfo dataInfo02 = buildGeneratorDataInfo("T01",
                        Lists.newArrayList("id", "num", "text", "ts"), GeneratorDataTool.generateData02());

                return Lists.newArrayList(dataInfo02);

            }

            @Override
            public List<String> generateRunSql() {

                String sql0="CREATE TABLE orders_04_01 (\n" +
                        "  order_id INT,\n" +
                        "  PRIMARY KEY (order_id) NOT ENFORCED\n" +
                        ")  WITH (\n" +
                        "  'connector' = 'jdbc',\n" +
                        "  'url' = 'jdbc:mysql://localhost:13306/mysql_etl_database_test03',\n" +
                        "  'username' = 'dataextract',\n" +
                        "  'password' = '1qaz2WSX',\n" +
                        "  'table-name' = 'orders_04_01'\n" +
                        ")";

                String sql1 = "CREATE TABLE printtSink ("
                        + "  order_id INT"
                        + ") WITH ("
                        + "  'connector'='print'"
                        + ")";

//                String sql2 = "select * from orders_04_01";
                String sql2 = "insert into printtSink select orders_04_01.order_id from T01 join orders_04_01 on 1=1";

                return Lists.newArrayList(sql0,sql1, sql2);
            }
        };

        flinkLocalRunHandler.executeSql();

    }




}
