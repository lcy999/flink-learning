package com.lcy.flinksql.demo;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.WindowReader;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.junit.Test;

import java.io.IOException;

/**
 * @author: lcy
 * @date: 2023/9/8
 **/
public class Test001 {

    @Test
    public void t01() throws IOException {
        ExecutionEnvironment bEnv   = ExecutionEnvironment.getExecutionEnvironment();
        ExistingSavepoint savepoint = Savepoint.load(bEnv, "file:/L:/test/test55_savepoints/savepoint-00078474-313c8a-933aa2faff21", new RocksDBStateBackend("file:/L:/test/test55_savepoints"));
        WindowReader<TimeWindow> window = savepoint.window(SlidingProcessingTimeWindows.of(Time.days(2), Time.days(1)));

        System.out.println(savepoint);
    }
}
