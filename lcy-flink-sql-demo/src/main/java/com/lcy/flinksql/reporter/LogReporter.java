package com.lcy.flinksql.reporter;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.MetricReporter;

import java.util.Map;

/**
 * @author: lcy
 * @date: 2023/8/3
 **/
@Slf4j
public class LogReporter implements MetricReporter {
    @Override
    public void open(MetricConfig metricConfig) {
        for(Map.Entry<Object, Object> item : metricConfig.entrySet()){
            log.info(item.getKey()+"---"+item.getValue());
        }

    }

    @Override
    public void close() {

    }

    @Override
    public void notifyOfAddedMetric(Metric metric, String s, MetricGroup metricGroup) {
        log.info(metric.toString());
    }

    @Override
    public void notifyOfRemovedMetric(Metric metric, String s, MetricGroup metricGroup) {
        log.info(metric.toString());
    }
}
