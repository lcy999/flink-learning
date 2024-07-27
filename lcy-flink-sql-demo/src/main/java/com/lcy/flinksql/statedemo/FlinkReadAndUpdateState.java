package com.lcy.flinksql.statedemo;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.BootstrapTransformation;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.OperatorTransformation;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class FlinkReadAndUpdateState {

    private static final String ckPath = "file:///E:\\OpenSource\\GitHub\\bigdata-learning\\常见大数据项目\\flink1.15-learning\\checkpoint\\b02f75ede7e3b093eb3b58bdd5906de3\\chk-10";


    @Test
    public void t_read_checkpoint_and_write_and_run() throws Exception {
        //1. 手动执行GenerateStateDemo，产生checkpoint
        //2. 读取ck文件状态数据, 并修改
        String readOperatorUid="my-uid";
        String readCkFile="L:\\test\\test55_savepoints\\run_checkpoint\\b246f15b2a983e0c852b8cbd4c29bf7f\\chk-3";
        stateWrite(readOperatorUid, readCkFile);

        //3. 产生新的ck文件L:\test\test55_savepoints\write_ck_by_file\**，
        //手动执行GenerateStateDemo，把这个注释打开“execution.savepoint.path”，并且设置对应新的ck路径

    }

    @Test
    public void t_write_and_generate_checkpoint_by_collection_data() throws Exception {
        Collection<KeyedState> data =
                Arrays.asList(new KeyedState("hive", 1L), new KeyedState("JasonLee1", 100L), new KeyedState("hhase", 3L));
        int maxParallelism = 128;

        ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<KeyedState> dataKeyedState = bEnv.fromCollection(data);

        BootstrapTransformation<KeyedState> transformation = OperatorTransformation
                .bootstrapWith(dataKeyedState)
                .keyBy(k -> k.key)
                .transform(new WriterFunction());

        Savepoint
                .create(new HashMapStateBackend(), maxParallelism)
                .withOperator("uid-test", transformation)
                .write("file:///L:\\test\\test55_savepoints\\write_ck_by_collection\\"+System.currentTimeMillis());

        bEnv.execute();
    }
    @Test
    public void t_read_checkpoint() throws Exception {
        //1. 手动执行GenerateStateDemo，产生checkpoint
        //2. 读取ck文件状态数据
        String operatorUid="my-uid";
        String ckFile="L:\\test\\test55_savepoints\\run_checkpoint\\4372df3ef94840392ff876a5ff1f7a6f\\chk-2";
        stateRead(operatorUid, ckFile);
    }

    /**
     * 从 ck 读取状态数据
     * @param ckPath
     * @throws Exception
     */
    public void stateRead(String uid,String ckPath) throws Exception {
        ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();
        bEnv.setParallelism(1);
        ExistingSavepoint savepoint = Savepoint.load(bEnv, ckPath, new HashMapStateBackend());
        DataSet<KeyedState> keyedState = savepoint.readKeyedState(uid, new ReaderFunction());
        List<KeyedState> keyedStates = keyedState.collect();
        for (KeyedState ks: keyedStates) {
            System.out.println(String.format("key: %s, value: %s", ks.key, ks.value));
        }
    }

    public static void stateWrite(String readUid,String readCkPath) throws Exception {
        int maxParallelism = 128;

        ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();

        ExistingSavepoint savepoint = Savepoint.load(bEnv, readCkPath, new HashMapStateBackend());
        DataSet<KeyedState> dataKeyedState = savepoint.readKeyedState(readUid, new ReaderFunction());
        DataSet<KeyedState> dataKeyedStateModify =dataKeyedState.map(keyedState -> {
            if(keyedState.key.equals("lcy-65")){
                keyedState.value=100L;
            }
            return keyedState;
        });

        BootstrapTransformation<KeyedState> transformation = OperatorTransformation
                .bootstrapWith(dataKeyedStateModify)
                .keyBy(k -> k.key)
                .transform(new WriterFunction());

        Savepoint
                .create(new HashMapStateBackend(), maxParallelism)
                .withOperator("uid-test", transformation)
                .write("file:///L:\\test\\test55_savepoints\\write_ck_by_file\\"+System.currentTimeMillis());

        bEnv.execute();
    }

    public static class WriterFunction extends KeyedStateBootstrapFunction<String, KeyedState> {
        ValueState<Long> state;
        @Override
        public void open(Configuration parameters) throws Exception {

            ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("lcystate", Types.LONG);
            state = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void processElement(KeyedState value, KeyedStateBootstrapFunction<String, KeyedState>.Context ctx) throws Exception {
            state.update(value.value);
        }
    }

    public static class ReaderFunction extends KeyedStateReaderFunction<String, KeyedState> {
        ValueState<Long> state;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("lcystate", Types.LONG);
            state = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void readKey(
                String key,
                Context ctx,
                Collector<KeyedState> out) throws Exception {

            KeyedState data = new KeyedState();
            data.key = key;
            data.value = state.value();
            out.collect(data);
        }
    }

    public static class KeyedState {
        public String key;
        public Long value;

        public KeyedState(String key, Long value) {
            this.key = key;
            this.value = value;
        }

        public KeyedState() {}
    }
}