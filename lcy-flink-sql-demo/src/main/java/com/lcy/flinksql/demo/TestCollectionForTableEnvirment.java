package com.lcy.flinksql.demo;

import com.lcy.flinksql.utils.FlinkLocalRunTableEnvirementHandler;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author: lcy
 * @date: 2023/7/27
 **/
public class TestCollectionForTableEnvirment {

    // 创建Logger对象
    private static final Logger log = LoggerFactory.getLogger(TestCollectionForColToRow.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        log.info(">>>>>>>>>>");
        System.out.println("<<<<<<<<<<<<<");
        Configuration conf = new Configuration();
        conf.setString("parallelism.default","4");

        FlinkLocalRunTableEnvirementHandler flinkLocalRunMoreHandler = new FlinkLocalRunTableEnvirementHandler(conf, true) {

            @Override
            public List<GeneratorDataInfo> generateData() {

                return Lists.newArrayList();
            }

            @Override
            public List<String> generateRunSql() {


                String sql1 = "CREATE TABLE upsertSink ("
                        + "  id INT"
                        + "  ,mod_num INT"
                        + ") WITH ("
                        + "  'connector'='print'"
                        + ")";

                String sql2 = "insert into upsertSink select id,mod(120,39) from autoCar where mod(120,39)=3";

                String sql3 = "CREATE TABLE upsertSink02 ("
                        + "  id INT"
                        + "  ,mod_num INT"
                        + ") WITH ("
                        + "  'connector'='print'"
                        + ")";

                String sql4 = "insert into upsertSink02 select id,mod(120,39) from autoCar where mod(120,39)=3";

                return Lists.newArrayList(sql1, sql3, sql4, sql2);
            }
        };

        flinkLocalRunMoreHandler.executeSql();

    }




}
