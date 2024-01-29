package com.lcy.flinksql.demo;

import com.lcy.flinksql.reporter.other.LogReporter;
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
public class TestMysqlCdc2PrintForLogReporter {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Configuration conf = new Configuration();
        conf.setString("metrics.reporter.mylog.class", LogReporter.class.getName());
        conf.setString("metrics.reporter.mylog.interval","5 SECONDS");

        FlinkLocalRunHandler flinkLocalRunHandler = new FlinkLocalRunHandler(conf) {

            @Override
            public void configEnv(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {

            }

            @Override
            public List<GeneratorDataInfo> generateData() {

                return Lists.newArrayList();
            }

            @Override
            public List<String> generateRunSql() {

                String sql0="CREATE TABLE oc_contract_all_t01 (\n" +
                        "  CONTRACT_ID INT,\n" +
                        "  MEMBER_BNAME STRING,\n" +
                        "  PRIMARY KEY (CONTRACT_ID) NOT ENFORCED\n" +
                        ")  WITH (\n" +
                        "  'connector' = 'mysql-cdc',\n" +
                        "  'hostname' = '172.17.26.209',\n" +
                        "  'port' = '13306',\n" +
                        "  'username' = 'dataextract',\n" +
                        "  'password' = '1qaz2WSX',\n" +
                        "  'database-name' = 'mysql_etl_database_test03',\n" +
                        "  'table-name' = 'oc_contract_all_t01',\n" +
                        "  'server-time-zone' = 'UTC',\n" +
                        "  'scan.startup.mode' = 'latest-offset'\n" +
                        ")";

                String sql1 = "CREATE TABLE upsertSink ("
                        + "  id INT,"
                        + "  MEMBER_BNAME STRING"
                        + ") WITH ("
                        + "  'connector'='print'"
                        + ")";

                String sql2 = "insert into upsertSink select * from oc_contract_all_t01";

                return Lists.newArrayList(sql0, sql1, sql2);
            }
        };

        flinkLocalRunHandler.executeSql();

    }




}
