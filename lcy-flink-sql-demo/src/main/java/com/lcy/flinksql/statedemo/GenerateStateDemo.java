package com.lcy.flinksql.statedemo;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2022-04-10
 */
public class GenerateStateDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration=new Configuration();
        configuration.setString("execution.savepoint.path","L:\\test\\test55_savepoints\\run_checkpoint\\4930e5c9f982a49396863458e48e1ac2\\chk-3");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        // 设置任务的最大并行度 也就是keyGroup的个数
        env.setMaxParallelism(128);
        //env.getConfig().setAutoWatermarkInterval(1000L);
        // 设置开启checkpoint
        env.enableCheckpointing(10000L);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///L:\\test\\test55_savepoints\\run_checkpoint");

        // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        DataStreamSource<Jason> dataStreamSource = env.addSource(new UserDefinedSource());
        dataStreamSource.keyBy(k -> k.getName())
                .process(new MyKeyedProcessFunction())
                .uid("my-uid")
                .print("local-print");

        env.execute();
    }

    public static class MyKeyedProcessFunction extends KeyedProcessFunction<String, Jason, Jason> implements CheckpointedFunction{
        private ValueState<Long> state;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> stateDescriptor = new ValueStateDescriptor<>("lcystate", Types.LONG);
            state = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void processElement(Jason value, KeyedProcessFunction<String, Jason, Jason>.Context ctx, Collector<Jason> out) throws Exception {
            if (state.value() != null) {
                System.out.println(value.getName()+ ": 状态里面有数据 :" + state.value());
                state.update(state.value()+1L);
            } else {
                state.update(1L);
            }
            out.collect(value);
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println(context);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println(context);
        }
    }
}