package com.lcy.flinksql.reporter;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.prometheus.AbstractPrometheusReporter;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * @author: lcy
 * @date: 2023/12/29
 **/
@PublicEvolving
@InstantiateViaFactory(
        factoryClassName =
                "com.lcy.flinksql.reporter.PayPrometheusPushGatewayReporterFactory")
public class PayPrometheusPushGatewayReporter extends AbstractPrometheusReporter implements Scheduled {

    private final PushGateway pushGateway;
    private final String jobName;
    private final Map<String, String> groupingKey;
    private final boolean deleteOnShutdown;
    private List<String> includeMetrics=new ArrayList<>();

    PayPrometheusPushGatewayReporter(
            String host,
            int port,
            String jobName,
            Map<String, String> groupingKey,
            final boolean deleteOnShutdown) {
        this.pushGateway = new PushGateway(host + ':' + port);
        this.jobName = Preconditions.checkNotNull(jobName);
        this.groupingKey = Preconditions.checkNotNull(groupingKey);
        String includeMetric = this.groupingKey.remove("includeMetric");
        if(includeMetric !=null){
            for (String item : includeMetric.split(",")) {
                includeMetrics.add(item);
            }
        }
        this.deleteOnShutdown = deleteOnShutdown;
    }

    @Override
    public void report() {
        try {

            pushGateway.push(filterMetrics(), jobName, groupingKey);
        } catch (Exception e) {
            log.warn(
                    "Failed to push metrics to PushGateway with jobName {}, groupingKey {}.",
                    jobName,
                    groupingKey,
                    e);
            e.printStackTrace();
        }
    }

    private CollectorRegistry filterMetrics() throws Exception {
        CollectorRegistry collectorRegistry = new CollectorRegistry(true);

        CollectorRegistry defaultRegistry = CollectorRegistry.defaultRegistry;
        Enumeration<Collector.MetricFamilySamples> metricFamilySamplesEnumeration = defaultRegistry.filteredMetricFamilySamples(new HashSet<>(includeMetrics));
        while (metricFamilySamplesEnumeration.hasMoreElements()){
            Collector.MetricFamilySamples metricFamilySamples = metricFamilySamplesEnumeration.nextElement();
            for (Collector.MetricFamilySamples.Sample sample : metricFamilySamples.samples) {
                Gauge gauge = Gauge.build()
                        .name(sample.name)
                        .help("help")
                        .labelNames(toArray(sample.labelNames))
                        .create();
                gauge.setChild(gaugeFrom(sample.value), toArray(sample.labelValues));
                collectorRegistry.register(gauge);
            }

        }
        /*Field namesToCollectorsField = defaultRegistry.getClass().getDeclaredField("namesToCollectors");
//        Field collectorsToNamesField = defaultRegistry.getClass().getDeclaredField("collectorsToNames");
        namesToCollectorsField.setAccessible(true);
//        collectorsToNamesField.setAccessible(true);
        Map<String, Collector> namesToCollectors= (Map<String, Collector>) namesToCollectorsField.get(defaultRegistry);
//        Map<Collector, List<String>> collectorsToNames= (Map<Collector, List<String>>) collectorsToNamesField.get(defaultRegistry);

        for (String metricName : namesToCollectors.keySet()) {
            if(!includeMetrics.contains(metricName)){
                continue;
            }

            Collector collector = namesToCollectors.get(metricName);
            collectorRegistry.register(collector);
        }*/

        return collectorRegistry;
    }

    private String[] toArray(List<String> list) {
        return list.toArray(new String[list.size()]);
    }

    private io.prometheus.client.Gauge.Child gaugeFrom(Object value) {
        return new io.prometheus.client.Gauge.Child() {
            @Override
            public double get() {
                if (value instanceof Double) {
                    return (double) value;
                }
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                if (value instanceof Boolean) {
                    return ((Boolean) value) ? 1 : 0;
                }
                System.out.println(String.format(
                        "Invalid type for Gauge ss: %s, only number types and booleans are supported by this reporter.",
                        value.getClass().getName()));
                return 0;
            }
        };
    }

    @Override
    public void close() {
        if (deleteOnShutdown && pushGateway != null) {
            try {
                pushGateway.delete(jobName, groupingKey);
            } catch (IOException e) {
                log.warn(
                        "Failed to delete metrics from PushGateway with jobName {}, groupingKey {}.",
                        jobName,
                        groupingKey,
                        e);
            }
        }
        super.close();
    }
}
