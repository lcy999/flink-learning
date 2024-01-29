package com.lcy.flinksql.demo;

import com.lcy.flinksql.reporter.other.PayPrometheusPushGatewayReporter;
import com.lcy.flinksql.utils.FlinkLocalRunHandler;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author: lcy
 * @date: 2023/7/27
 **/
public class TestCollectionForPrometheus {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setString("metrics.reporter.promgateway.class", PayPrometheusPushGatewayReporter.class.getName());
        conf.setString("metrics.reporter.promgateway.interval","5 SECONDS");
        conf.setString("metrics.reporter.promgateway.host","localhost");
        conf.setString("metrics.reporter.promgateway.port","19091");
        conf.setString("metrics.reporter.promgateway.groupingKey","includeMetric=flink_jobmanager_Status_JVM_CPU_Load,flink_jobmanager_Status_JVM_CPU_Time");
        conf.setString("metrics.reporter.promgateway.jobName","localjob_03_"+System.currentTimeMillis());

        FlinkLocalRunHandler flinkLocalRunHandler = new FlinkLocalRunHandler(conf, true) {

            @Override
            public List<GeneratorDataInfo> generateData() {

                return Lists.newArrayList();
            }

            @Override
            public List<String> generateRunSql() {


                String sql1 = "CREATE TABLE upsertSink ("
                        + "  id INT"
                        + ") WITH ("
                        + "  'connector'='print'"
                        + ")";

                String sql2 = "insert into upsertSink select id from autoCar";

                return Lists.newArrayList(sql1, sql2);
            }
        };

        flinkLocalRunHandler.executeSql();

    }




}
