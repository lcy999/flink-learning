package com.lcy.flinksql.demo;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.util.Collector;

/**
 * @author: lcy
 * @date: 2024/3/16
 **/
public class ReadStateFile {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment bEnv = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint savepoint = Savepoint.load(bEnv, "file:/L:/test/test55_savepoints/ck-09/_metadata", new RocksDBStateBackend("file:/L:/test/test55_savepoints"));

        savepoint
                .readKeyedState("31fecc704f203e7f253c40c6abb946c0", new ReadAllKeyedState())
                .collect()
                .forEach(System.out::println);
    }

    static class ReadAllKeyedState extends KeyedStateReaderFunction<Integer, Tuple2<Integer, Integer>> {

        transient ValueState<Integer> state;


        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>("state_name", Types.INT);
            state = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void readKey(Integer key, Context ctx, Collector<Tuple2<Integer, Integer>> out) throws Exception {
            Integer value = state.value();
            if (value != null) {
                out.collect(Tuple2.of(key, value));
            }
        }
    }
}
