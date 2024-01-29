/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lcy.flinksql.reporter.victoriametric;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.*;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.*;

import static com.lcy.flinksql.reporter.victoriametric.VictoriaMetricReporterOptions.*;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.GROUPING_KEY;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.JOB_NAME;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.RANDOM_JOB_NAME_SUFFIX;

/** {@link MetricReporter} that exports {@link Metric Metrics} via InfluxDB. */
@InstantiateViaFactory(
        factoryClassName = "com.lcy.flinksql.reporter.victoriametric.VictoriaMetricReporterFactory")
public class VictoriaMetricReporter extends AbstractVictoriaMetricReporter<VictoriaMetricInfo> implements Scheduled {

    private String jobName;
    private String vmImportUrl;
    private Map<String, String> groupingKey;
    private boolean isFilterMetric;
    private String filterMetricUrl;


    public VictoriaMetricReporter() {
        super(new VictoriaMetricInfoProvider());
    }

    @Override
    public void open(MetricConfig metricConfig) {
        String host = metricConfig.getString(HOST.key(), HOST.defaultValue());
        Integer port = metricConfig.getInteger(PORT.key(), PORT.defaultValue());

        if (host == null || host.isEmpty() || port < 1) {
            throw new IllegalArgumentException(
                    "Invalid host/port configuration. Host: " + host + " Port: " + port);
        }

        String vmUrlFormat = "http://%s:%s/api/v1/import/prometheus";
        vmImportUrl = String.format(vmUrlFormat,host, port);

        String configuredJobName = metricConfig.getString(JOB_NAME.key(), JOB_NAME.defaultValue());
        boolean randomSuffix =
                metricConfig.getBoolean(
                        RANDOM_JOB_NAME_SUFFIX.key(), RANDOM_JOB_NAME_SUFFIX.defaultValue());

        jobName = configuredJobName;
        if (randomSuffix) {
            jobName = configuredJobName + new AbstractID();
        }

        groupingKey = parseGroupingKey(
                metricConfig.getString(GROUPING_KEY.key(), GROUPING_KEY.defaultValue()));

        isFilterMetric = metricConfig.getBoolean(FILTER_METRIC.key(), FILTER_METRIC.defaultValue());
        filterMetricUrl = metricConfig.getString(FILTER_METRIC_URL.key(), FILTER_METRIC_URL.defaultValue());


        log.info(
                "Configured VM Reporter with {host:{}, port:{}, jobName:{}, groupingKey:{},isFilterMetric:{}, filterMetricUrl:{} }",
                host
                ,port
                ,jobName
                ,groupingKey
                ,isFilterMetric
                ,filterMetricUrl
        );
    }

    @Override
    public void close() {

    }

    @Override
    public void report() {
        try {
            for (Map.Entry<Gauge<?>, VictoriaMetricInfo> entry : gauges.entrySet()) {
                report.point(MetricMapper.map(entry.getValue(), timestamp, entry.getKey()));
            }

            for (Map.Entry<Counter, VictoriaMetricInfo> entry : counters.entrySet()) {
                report.point(MetricMapper.map(entry.getValue(), timestamp, entry.getKey()));
            }

            for (Map.Entry<Histogram, VictoriaMetricInfo> entry : histograms.entrySet()) {
                report.point(MetricMapper.map(entry.getValue(), timestamp, entry.getKey()));
            }

            for (Map.Entry<Meter, VictoriaMetricInfo> entry : meters.entrySet()) {
                report.point(MetricMapper.map(entry.getValue(), timestamp, entry.getKey()));
            }
        } catch (ConcurrentModificationException | NoSuchElementException e) {
            // ignore - may happen when metrics are concurrently added or removed
            // report next time
            return null;
        }
    }


    @VisibleForTesting
    private Map<String, String> parseGroupingKey(final String groupingKeyConfig) {
        if (!groupingKeyConfig.isEmpty()) {
            Map<String, String> groupingKey = new HashMap<>();
            String[] kvs = groupingKeyConfig.split(";");
            for (String kv : kvs) {
                int idx = kv.indexOf("=");
                if (idx < 0) {
                    log.warn("Invalid prometheusPushGateway groupingKey:{}, will be ignored", kv);
                    continue;
                }

                String labelKey = kv.substring(0, idx);
                String labelValue = kv.substring(idx + 1);
                if (StringUtils.isNullOrWhitespaceOnly(labelKey)
                        || StringUtils.isNullOrWhitespaceOnly(labelValue)) {
                    log.warn(
                            "Invalid groupingKey {labelKey:{}, labelValue:{}} must not be empty",
                            labelKey,
                            labelValue);
                    continue;
                }
                groupingKey.put(labelKey, labelValue);
            }

            return groupingKey;
        }

        return Collections.emptyMap();
    }

    private static boolean isValidHost(String host) {
        return host != null && !host.isEmpty();
    }
}
