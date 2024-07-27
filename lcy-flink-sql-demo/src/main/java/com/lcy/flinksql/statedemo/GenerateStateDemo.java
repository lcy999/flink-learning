package com.lcy.flinksql.statedemo;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
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
//        configuration.setString("execution.savepoint.path","L:\\test\\test55_savepoints\\run_checkpoint\\b246f15b2a983e0c852b8cbd4c29bf7f\\chk-3");
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
                .process(new KeyedProcessFunction<String, Jason, Jason>() {
                    private ValueState<Integer> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("state", Types.INT);
                        state = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void processElement(Jason value, KeyedProcessFunction<String, Jason, Jason>.Context ctx, Collector<Jason> out) throws Exception {
                        if (state.value() != null) {
                            System.out.println(value.getName()+ ": 状态里面有数据 :" + state.value());
                            state.update(state.value()+1);
                        } else {
                            state.update(1);
                        }
                        out.collect(value);
                    }
                }).uid("my-uid")
                .print("local-print");

        env.execute();
    }
}