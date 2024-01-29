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

    private String database;
    private String retentionPolicy;
    private InfluxDB.ConsistencyLevel consistency;
    private InfluxDB influxDB;

    public VictoriaMetricReporter() {
        super(new VictoriaMetricInfoProvider());
    }

    @Override
    public void open(MetricConfig metricConfig) {
        String host = metricConfig.getString(HOST.key(), HOST.defaultValue());
        Integer port = metricConfig.getInteger(PORT.key(), PORT.defaultValue());

        String configuredJobName = metricConfig.getString(JOB_NAME.key(), JOB_NAME.defaultValue());
        boolean randomSuffix =
                metricConfig.getBoolean(
                        RANDOM_JOB_NAME_SUFFIX.key(), RANDOM_JOB_NAME_SUFFIX.defaultValue());

        Map<String, String> groupingKey =
                parseGroupingKey(
                        metricConfig.getString(GROUPING_KEY.key(), GROUPING_KEY.defaultValue()));

        if (host == null || host.isEmpty() || port < 1) {
            throw new IllegalArgumentException(
                    "Invalid host/port configuration. Host: " + host + " Port: " + port);
        }

        String jobName = configuredJobName;
        if (randomSuffix) {
            jobName = configuredJobName + new AbstractID();
        }


        log.info(
                "Configured InfluxDBReporter with {host:{}, port:{}, db:{}, retentionPolicy:{} and consistency:{}}",
                host,
                port,
                database,
                retentionPolicy,
                consistency.name());
    }

    @Override
    public void close() {
        if (influxDB != null) {
            influxDB.close();
            influxDB = null;
        }
    }

    @Override
    public void report() {
        BatchPoints report = buildReport();
        if (report != null) {
            influxDB.write(report);
        }
    }

    @Nullable
    private BatchPoints buildReport() {
        Instant timestamp = Instant.now();
        BatchPoints.Builder report = BatchPoints.database(database);
        report.retentionPolicy(retentionPolicy);
        report.consistency(consistency);
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
        return report.build();
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
