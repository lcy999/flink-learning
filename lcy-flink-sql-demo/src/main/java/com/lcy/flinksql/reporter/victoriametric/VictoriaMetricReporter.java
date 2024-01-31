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

import com.lcy.flinksql.utils.HttpClientUtil;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.InstantiateViaFactory;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.lcy.flinksql.reporter.victoriametric.VictoriaMetricReporterOptions.*;

/** {@link MetricReporter} that exports {@link Metric Metrics} via InfluxDB. */
@InstantiateViaFactory(
        factoryClassName = "com.lcy.flinksql.reporter.victoriametric.VictoriaMetricReporterFactory")
public class VictoriaMetricReporter extends AbstractVictoriaMetricReporter<VictoriaMetricInfo> implements Scheduled {

    private String jobName;
    private String vmImportUrl;
    private Map<String, String> groupingKey;
    private boolean isFilterMetric;
    private String filterMetricUrl;
    private String groupKeyString;


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
        StringBuilder groupKeyBuilder=new StringBuilder();
        Object[] keyGrouping = groupingKey.keySet().toArray();
        for(int i=0;i < keyGrouping.length; i++){
            String lableKey = (String) keyGrouping[i];
            String lableValue= groupingKey.get(keyGrouping[i]);
            if(i== keyGrouping.length-1){
                groupKeyBuilder.append(String.format("%s=\"%s\"",lableKey, lableValue));
            }else{
                groupKeyBuilder.append(String.format("%s=\"%s\",",lableKey, lableValue));
            }
        }
        groupKeyString = groupKeyBuilder.toString();

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
            StringBuilder metricBuilder= new StringBuilder();

            for (Map.Entry<Gauge<?>, VictoriaMetricInfo> entry : gauges.entrySet()) {
                metricBuilder.append(buildVMPushMetric(entry.getKey(), entry.getValue())+"\n");
            }

            for (Map.Entry<Counter, VictoriaMetricInfo> entry : counters.entrySet()) {
                metricBuilder.append(buildVMPushMetric(entry.getKey(), entry.getValue())+"\n");
            }

            for (Map.Entry<Histogram, VictoriaMetricInfo> entry : histograms.entrySet()) {
                metricBuilder.append(buildVMPushMetric(entry.getKey(), entry.getValue())+"\n");
            }

            for (Map.Entry<Meter, VictoriaMetricInfo> entry : meters.entrySet()) {
                metricBuilder.append(buildVMPushMetric(entry.getKey(), entry.getValue())+"\n");
            }

            HttpClientUtil.postJson(vmImportUrl, metricBuilder.toString(), null);
        } catch (Exception e) {
            log.warn(
                    "Failed to push metrics to VictoriaMetric Server with jobName {}, groupingKey {}.",
                    jobName,
                    groupingKey,
                    e);
        }
    }

    @VisibleForTesting
    String buildVMPushMetric(Metric metric, VictoriaMetricInfo vmMetricInfo){
        StringBuilder lableBuilder=new StringBuilder();

        Object[] keyTags = vmMetricInfo.getTags().keySet().toArray();
        for(int i=0; i< keyTags.length; i++){
            String lableKey = (String) keyTags[i];
            String lableValue = vmMetricInfo.getTags().get(lableKey);
            if(i== keyTags.length-1){
                lableBuilder.append(String.format("%s=\"%s\"",lableKey, lableValue));
            }else{
                lableBuilder.append(String.format("%s=\"%s\",",lableKey, lableValue));
            }
        }

        if(groupKeyString.length() >0){
            lableBuilder.append(","+groupKeyString);
        }

        lableBuilder.append(","+ String.format("job=\"%s\"", jobName));

        String lableString= lableBuilder.toString();
        String metricValue = getMetricValue(metric);

        return String.format("%s{%s} %s", vmMetricInfo.getName(), lableString, metricValue);
    }

    public String getMetricValue(Object metric) {
        Object value= null;
        if(metric instanceof Gauge){
            value = ((Gauge)metric).getValue();
        }else if(metric instanceof Counter){
            Counter counter= (Counter) metric;
            value= counter.getCount();
        }else if(metric instanceof Meter){
            Meter meter= (Meter) metric;
            value= meter.getRate();
        }

        if (value == null) {
            log.info("metric {} is null-valued, defaulting to 0.", metric);
            value= "0";
        }
        return String.valueOf(value);
    }




    @VisibleForTesting
    Map<String, String> parseGroupingKey(final String groupingKeyConfig) {
        if (!groupingKeyConfig.isEmpty()) {
            Map<String, String> groupingKey = new HashMap<>();
            String[] kvs = groupingKeyConfig.split(";");
            for (String kv : kvs) {
                int idx = kv.indexOf("=");
                if (idx < 0) {
                    log.warn("Invalid victoriaMetric groupingKey:{}, will be ignored", kv);
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

}
