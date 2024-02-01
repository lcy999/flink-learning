package com.lcy.flinksql.utils;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.delegation.ExecutorFactory;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.ComponentFactoryService;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;

import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author: lcy
 * @date: 2023/7/27
 **/
@Slf4j
public abstract class FlinkLocalRunTableEnvirementHandler {

    public static final String UID_DELIMITER = "#";

    private TableEnvironment tEnv;

    public FlinkLocalRunTableEnvirementHandler(){
        this(null);
    }

    public FlinkLocalRunTableEnvirementHandler(Configuration configuration){
        tEnv= configureTableEnv(configuration);
    }

    public FlinkLocalRunTableEnvirementHandler(Configuration configuration, boolean isAutoCar){
        this(configuration);
        if(isAutoCar){
            tEnv.createTemporaryView("autoCar", tEnv.fromValues(1001, 1002, 1003).as("id"));

        }

    }

    private static TableEnvironment configureTableEnv(Configuration confInput) {
        Configuration conf= confInput==null? new Configuration(): confInput;
        //设置prometheus的job name
        SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMddHHmmss");
        String curTime = sdf.format(new Date());
        conf.setString("metrics.reporter.promgateway.jobName", "local_test_"+curTime+"_");

        conf.setString("execution.runtime-mode","streaming");
        conf.setString("table.planner","blink");

        conf.setString("state.backend","filesystem");

        conf.setString("state.checkpoints.dir", "./checkpoints_local");
        conf.setString("execution.checkpointing.mode", "EXACTLY_ONCE");
        conf.setString("execution.checkpointing.externalized-checkpoint-retention", "RETAIN_ON_CANCELLATION");

        conf.setString("restart-strategy","fixed-delay");
        conf.setString("restart-strategy.fixed-delay.delay","1 min");
        conf.setString("restart-strategy.fixed-delay.attempts","10");

        conf.setString("table.exec.source.idle-timeout","5 s");
        conf.setString("table.local-time-zone","Asia/Shanghai");


        TableEnvironment tableEnv = TableEnvironment.create(conf);



        return tableEnv;

    }


    public abstract List<GeneratorDataInfo> generateData();

    public abstract List<String> generateRunSql();

    public void executeSql(){
        List<GeneratorDataInfo> generatorDataInfos = generateData();

        List<String> sqls = generateRunSql();
        /*for (String sql : sqls) {
            try {
                tEnv.executeSql(sql);
            }catch (SqlParserException sqlParserException){
                log.error("parse failed sql: "+sql);
                sqlParserException.printStackTrace();
            }

        }*/

        Executor executor = lookupExecutor(tEnv);

        String jobName="local_test_name";
        String sqlIds="local_test_sqlids";
        Pipeline pipeline = topologicalIsolation(tEnv, executor, jobName, sqlIds, sqls);

        try {
            JobClient jobClient = executor.executeAsync(pipeline);
            log.info("JobID: {}", jobClient.getJobID());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static Executor lookupExecutor(TableEnvironment executionEnvironment) {

        try {
            Map<String, String> executorProperties = new HashMap<>();

            Executor executor =
                    ComponentFactoryService.find(ExecutorFactory.class, executorProperties)
                            .create(executorProperties);

            return executor;
        } catch (Exception e) {
            throw new TableException(
                    "Failed when look up executor: ",
                    e);
        }
    }

    private static Executor lookupExecutor(StreamExecutionEnvironment executionEnvironment) {

        Map<String, String> executorProperties = new HashMap<>();
        try {
            ExecutorFactory executorFactory =
                    ComponentFactoryService.find(ExecutorFactory.class, executorProperties);
            Method createMethod =
                    executorFactory
                            .getClass()
                            .getMethod("create", Map.class, StreamExecutionEnvironment.class);

            return (Executor)
                    createMethod.invoke(executorFactory, executorProperties, executionEnvironment);
        } catch (Exception e) {
            throw new TableException(
                    "Could not instantiate the executor. Make sure a planner module is on the classpath",
                    e);
        }
    }

    private static Pipeline topologicalIsolation(TableEnvironment tableEnv, Executor executor,
                                                 String jobName, String sqlIds, List<String> sqls) {

        Parser parser = ((TableEnvironmentImpl) tableEnv).getParser();

        List<ModifyOperation> modifies = new ArrayList<>();

        List<String> dmlSqls = new ArrayList<>();
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        for (String sql : sqls) {
            sql = sql.trim();
            List<Operation> operations = parser.parse(sql);
            Operation op = operations.get(0);
            if (op instanceof ModifyOperation) {
                System.out.println("dmlSql：" + sql);
                dmlSqls.add(sql);
                modifies.add((ModifyOperation) op);
            } else {
                System.out.println("ddlSql：" + sql);
                tableEnv.executeSql(sql);
            }
        }

        List<Transformation<?>> transformationsAll = new ArrayList<>();
        dealTransformations(modifies, transformationsAll, tableEnv, dmlSqls, sqlIds);

        Pipeline pipeline = executor.createPipeline(transformationsAll, tableEnv.getConfig(), jobName);
        return pipeline;
    }

    private static void dealTransformations(List<ModifyOperation> modifies, List<Transformation<?>> transformationsAll,
                                            TableEnvironment tableEnv, List<String> dmlSqls, String sqlIds) {
        int i = 0;
        List<String> uids = generateUIDOrArgsGet(dmlSqls, sqlIds);
        for (ModifyOperation operation : modifies) {
            String uid = uids.get(i);
            List<Transformation<?>> transformations = ((TableEnvironmentImpl) tableEnv).getPlanner().translate(Arrays.asList(operation));
            transformationsAll.addAll(transformations);
            for (Transformation<?> transformation : transformations) {
                transformation.setUid(uid);
                String deep = "0";
                factorial(transformation, uid, deep);
            }
            i++;
        }
    }

    private static void factorial(Transformation transformation, String uid, String deep) {
        List<Transformation<?>> transformations = transformation.getInputs();

        if (transformations.size() != 0) {
            for (int i=0;i<transformations.size();i++) {
                Transformation<?> tf = transformations.get(i);
                String curDeep=deep;
                if(transformations.size()> 1){
                    curDeep= addDepIndexChar(deep, i);
                }
                String curUid = uid.concat(UID_DELIMITER + curDeep);
                tf.setUid(curUid);
                String subDeep = addDep(curDeep, i, transformations.size());
                factorial(tf, curUid, subDeep);
            }
        } else {
            System.out.println("uid:" + uid);
        }
    }

    private static String addDep(String deep, int sourceIndex, int sourceCount){
        deep= deep.split("_")[0];
        deep=String.valueOf(Integer.parseInt(deep)+1);
        //来源存在并行的，例如union
        if(sourceCount>1){
            deep= addDepIndexChar(deep, sourceIndex);
        }

        return deep;
    }

    private static String addDepIndexChar(String deep, int sourceIndex){
        char indexChar= (char) ('a' + sourceIndex);
        deep+= "_"+indexChar;

        return deep;
    }

    //SQL_HASH = 定长SQL_ID + MD5(SQL)
    //计算函数HASH = SQL_HASH+ DEEP
    private static List<String> generateUIDOrArgsGet(List<String> dmlSqls, String sqlIds) {

        List<String> uids = new ArrayList<>();
        Map<String, String> sqlmd5InMap = new HashedMap();
        Map<String, String> sqlmd5AllMap = new HashedMap();
        for (int i = 0; i < dmlSqls.size(); i++) {
            String sql = dmlSqls.get(i);
            String sqlTrim = sql.replaceAll("--.*", "").replaceAll("\r\n", " ").replaceAll("\n", " ").replace("\t", " ").trim();
            System.out.println("generateUIDOrArgsGet sql:" + sql);
            String sqlMd5 = DigestUtils.md5Hex(sqlTrim);
            uids.add(i, "00".concat(UID_DELIMITER).concat(sqlMd5));
            sqlmd5InMap.put(sqlMd5, sql);
            sqlmd5AllMap.put(sqlMd5, sql);
        }
        sqlmd5AllMap.forEach((key, value) -> {
            if (sqlmd5InMap.containsKey(key)) {
                String uid = generateUID(value);
                uids.add(uid);
            }
        });
        return uids;
    }

    private static String generateUID(String sql) {
        String sqlId = UUID.randomUUID().toString().concat(UID_DELIMITER);
        String sqlMd5Code = DigestUtils.md5Hex(sql);
        String uid = sqlId.concat(sqlMd5Code);
        return uid;
    }


    protected GeneratorDataInfo buildGeneratorDataInfo(String tableName, List<String> fieldNames, List<?> data){

        List<Expression> expressions = fieldNames.stream().map(fname -> $(fname)).collect(Collectors.toList());
        return GeneratorDataInfo.builder()
                .tableName(tableName)
                .fieldNames(expressions)
                .data(data)
                .build();
    }

    @Data
    @Builder
    public static class GeneratorDataInfo {
        private String tableName;
        private List<?> data;
        private List<Expression> fieldNames;
        
    }


}
