package com.lcy.flinksql.demo;

import com.lcy.flinksql.utils.FlinkLocalRunHandler;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author: lcy
 * @date: 2023/7/27
 **/
public class TestCollectionForVmReporter {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMddHHmmss");
        String curTime = sdf.format(new Date());
        System.out.println("curTime flag:"+curTime);
        Configuration conf = new Configuration();
        conf.setString("metrics.reporter.victoriametric.class", "com.lcy.flinksql.reporter.victoriametric.VictoriaMetricReporter");
        conf.setString("metrics.reporter.victoriametric.interval","5 SECONDS");
        conf.setString("metrics.reporter.victoriametric.host","localhost");
        conf.setString("metrics.reporter.victoriametric.port","18429");
        conf.setString("metrics.reporter.victoriametric.groupingKey","k9=v9");
        conf.setString("metrics.reporter.victoriametric.jobName","localjob_"+curTime+"_");
        conf.setString("metrics.reporter.victoriametric.metricNameSuffix","_localtest01");
//        conf.setString("metrics.reporter.victoriametric.filterMetric","true");
//        conf.setString("metrics.reporter.victoriametric.reportCountForRequestFilterInfo","5");
//        conf.setString("metrics.reporter.victoriametric.filterMetricUrl","http://localhost:8082/streamingSql/queryReporterMetric?metricName=");

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

                String sql3 = "CREATE TABLE upsertSink02 ("
                        + "  id INT"
                        + ") WITH ("
                        + "  'connector'='print'"
                        + ")";

                String sql4 = "insert into upsertSink02 select id from autoCar";

                return Lists.newArrayList(sql1, sql2, sql3, sql4);
            }
        };

        flinkLocalRunHandler.executeSql();

    }




}
