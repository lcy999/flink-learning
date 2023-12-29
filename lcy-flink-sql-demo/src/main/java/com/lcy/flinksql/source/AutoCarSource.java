package com.lcy.flinksql.source;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.Random;

/**
 * @author: lcy
 * @date: 2023/12/29
 **/
public class AutoCarSource implements SourceFunction<Tuple4<Integer, Integer, Double, Long>> {

    private static final long serialVersionUID = 1L;
    private Integer[] speeds;
    private Double[] distances;

    private Random rand = new Random();

    private volatile boolean isRunning = true;

    private AutoCarSource(int numOfCars) {
        speeds = new Integer[numOfCars];
        distances = new Double[numOfCars];
        Arrays.fill(speeds, 50);
        Arrays.fill(distances, 0d);
    }

    public static AutoCarSource create(int cars) {
        return new AutoCarSource(cars);
    }

    @Override
    public void run(SourceContext<Tuple4<Integer, Integer, Double, Long>> ctx)
            throws Exception {

        while (isRunning) {
            Thread.sleep(1000);
            for (int carId = 0; carId < speeds.length; carId++) {
                if (rand.nextBoolean()) {
                    speeds[carId] = Math.min(100, speeds[carId] + 5);
                } else {
                    speeds[carId] = Math.max(0, speeds[carId] - 5);
                }
                distances[carId] += speeds[carId] / 3.6d;
                Tuple4<Integer, Integer, Double, Long> record =
                        new Tuple4<>(
                                carId,
                                speeds[carId],
                                distances[carId],
                                System.currentTimeMillis());
                ctx.collect(record);
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
