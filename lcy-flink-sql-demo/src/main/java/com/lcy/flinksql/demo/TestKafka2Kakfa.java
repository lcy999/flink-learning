package com.lcy.flinksql.demo;

import com.lcy.flinksql.utils.FlinkLocalRunHandler;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author: lcy
 * @date: 2023/7/27
 **/
public class TestKafka2Kakfa {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Configuration conf = new Configuration();
//        conf.setString("table.exec.emit.early-fire.enabled","true");
//        conf.setString("table.exec.emit.early-fire.delay","5000ms");

        FlinkLocalRunHandler flinkLocalRunHandler = new FlinkLocalRunHandler(conf) {

            @Override
            public void configEnv(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
                tEnv.getConfig().getConfiguration().setString("table.exec.emit.early-fire.enabled", "true");
                tEnv.getConfig().getConfiguration().setString("table.exec.emit.early-fire.delay", "5000ms");
            }

            @Override
            public List<GeneratorDataInfo> generateData() {

                return Lists.newArrayList();
            }

            @Override
            public List<String> generateRunSql() {

                String sql0="CREATE TABLE orders_04 (\n" +
                        "  product_id INT,\n" +
                        "  price double,\n" +
                        "  proctime as PROCTIME()\n" +
                        ")  WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'test_res_03',\n" +
                        "  'properties.bootstrap.servers' = 'qione01:9092',\n" +
                        "  'format' = 'json',\n" +
                        "  'properties.group.id' = 'cs_orders_04',\n" +
                        "  'scan.startup.mode' = 'latest-offset'\n" +
                        ")";

                String sql1 = "CREATE TABLE index_result_update2 (\n" +
                        "  product_id INT,\n" +
                        "  price double,\n" +
                        "  PRIMARY KEY (product_id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "  'connector' = 'upsert-kafka',\n" +
                        "  'topic' = 'test_res_04',\n" +
                        "  'properties.bootstrap.servers' = 'qione01:9092',\n" +
                        "  'key.format' = 'json',\n" +
                        "  'value.format' = 'json'\n" +
                        ")";

                String sql2 = "insert into \n" +
                        "index_result_update2\n" +
                        "select \n" +
                        "product_id\n" +
                        ", sum(price)\n" +
                        "from orders_04\n" +
                        "group by product_id, HOP(proctime, INTERVAL '1' MINUTE, INTERVAL '2' MINUTE)";

                String sql3="CREATE TABLE sink_print (\n" +
                        "  product_id INT,\n" +
                        "  price double,\n" +
                        "  PRIMARY KEY (product_id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "  'connector' = 'print'\n" +
                        ")";


                String sql5 = "insert into \n" +
                        "sink_print\n" +
                        "select \n" +
                        "product_id\n" +
                        ", sum(price)\n" +
                        "from orders_04\n" +
                        "group by product_id, HOP(proctime, INTERVAL '1' MINUTE, INTERVAL '2' MINUTE)";

                return Lists.newArrayList(sql0, sql1,sql2, sql3,sql5);
            }
        };

        flinkLocalRunHandler.executeSql();

    }




}
