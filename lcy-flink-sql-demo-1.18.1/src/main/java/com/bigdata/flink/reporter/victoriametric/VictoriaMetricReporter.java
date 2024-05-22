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

package com.bigdata.flink.reporter.victoriametric;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.flink.reporter.utils.HttpClientUtil;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static com.bigdata.flink.reporter.victoriametric.VictoriaMetricReporterOptions.FILTER_LABEL_VALUE_CHARACTER;
import static com.bigdata.flink.reporter.victoriametric.VictoriaMetricReporterOptions.FILTER_METRIC;
import static com.bigdata.flink.reporter.victoriametric.VictoriaMetricReporterOptions.FILTER_METRIC_URL;
import static com.bigdata.flink.reporter.victoriametric.VictoriaMetricReporterOptions.GROUPING_KEY;
import static com.bigdata.flink.reporter.victoriametric.VictoriaMetricReporterOptions.HOST;
import static com.bigdata.flink.reporter.victoriametric.VictoriaMetricReporterOptions.JOB_NAME;
import static com.bigdata.flink.reporter.victoriametric.VictoriaMetricReporterOptions.METRIC_NAME_SUFFIX;
import static com.bigdata.flink.reporter.victoriametric.VictoriaMetricReporterOptions.PORT;
import static com.bigdata.flink.reporter.victoriametric.VictoriaMetricReporterOptions.RANDOM_JOB_NAME_SUFFIX;
import static com.bigdata.flink.reporter.victoriametric.VictoriaMetricReporterOptions.REPORT_COUNT_FOR_REQUEST_FILTER_INFO;
import static com.bigdata.flink.reporter.victoriametric.VictoriaMetricReporterOptions.VICTORIA_METRIC_SERVER_MODE;

/** {@link MetricReporter} that exports {@link Metric Metrics} via InfluxDB. */
public class VictoriaMetricReporter extends AbstractVictoriaMetricReporter<VictoriaMetricInfo> implements Scheduled {

    private String jobName;
    private String vmImportUrl;
    private Map<String, String> groupingKey;
    private boolean isFilterMetric;
    private String filterMetricUrl;
    private String groupKeyString;
    private String metricNameSuffix;
    private int currentReporterCounterForFilterInfo= 0;

    private static final Pattern UNALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9:_]");
    private static final CharacterFilter CHARACTER_FILTER =
            new CharacterFilter() {
                @Override
                public String filterCharacters(String input) {
                    return replaceInvalidChars(input);
                }
            };

    private static final char SCOPE_SEPARATOR = '_';
    private static final String SCOPE_PREFIX = "flink" + SCOPE_SEPARATOR;
    private boolean isPrintMetric= true;
    private boolean filterLableValueCharacter;
    private int reportCountForRequestFilterInfo;
    private FilterMetricInfo filterMetricInfo= new FilterMetricInfo();


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

        String victoriaMetricServerMode = metricConfig.getString(VICTORIA_METRIC_SERVER_MODE.key(), VICTORIA_METRIC_SERVER_MODE.defaultValue());

        String vmUrlFormat = "http://%s:%s/insert/0/prometheus/api/v1/import/prometheus";
        if(victoriaMetricServerMode.toLowerCase().equals("single")){
            vmUrlFormat = "http://%s:%s/api/v1/import/prometheus";
        }
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
        metricNameSuffix = metricConfig.getString(METRIC_NAME_SUFFIX.key(), METRIC_NAME_SUFFIX.defaultValue());
        filterLableValueCharacter = metricConfig.getBoolean(FILTER_LABEL_VALUE_CHARACTER.key(), FILTER_LABEL_VALUE_CHARACTER.defaultValue());
        reportCountForRequestFilterInfo = metricConfig.getInteger(REPORT_COUNT_FOR_REQUEST_FILTER_INFO.key(), REPORT_COUNT_FOR_REQUEST_FILTER_INFO.defaultValue());

        log.info(
                "Configured VM Reporter with {host:{}, port:{}, jobName:{}" +
                        ", groupingKey:{},isFilterMetric:{}, filterMetricUrl:{},metricNameSuffix:{} " +
                        ",filterLableValueCharacter:{},reportCountForRequestFilterInfo:{} }",
                host
                ,port
                ,jobName
                ,groupingKey
                ,isFilterMetric
                ,filterMetricUrl
                ,metricNameSuffix
                ,filterLableValueCharacter
                ,reportCountForRequestFilterInfo
        );

        if(isFilterMetric){
            Preconditions.checkNotNull(filterMetricUrl);
            Preconditions.checkArgument(reportCountForRequestFilterInfo!= REPORT_COUNT_FOR_REQUEST_FILTER_INFO.defaultValue()
                    ,"Please set reportCountForRequestFilterInfo when filter url");
            requestFilterMetricInfo();
        }

    }

    @Override
    public void close() {

    }

    @Override
    public void report() {
        try {
            StringBuilder metricBuilder= new StringBuilder();

            Map<Metric, VictoriaMetricInfo> reportMetricData = getReportMetricData();

            int reportMetricNum=0;
            for (Map.Entry<Metric, VictoriaMetricInfo> entry : reportMetricData.entrySet()) {
                Metric metric = entry.getKey();
                VictoriaMetricInfo victoriaMetricInfo = entry.getValue();
                String fullMetricName = getFullMetricName(victoriaMetricInfo.getName());
                if(filterMetricInfo.filterMetric(fullMetricName)){
                    metricBuilder.append(buildVMPushMetric(metric, victoriaMetricInfo)+"\n");
                    reportMetricNum++;
                }
            }

            if(reportMetricNum==0){
                log.warn("Report metric num is zero, the filer reuqest url:{}", filterMetricUrl);
            }else{
                HttpClientUtil.postJson(vmImportUrl, metricBuilder.toString(), null);
            }

            if(isFilterMetric){
                currentReporterCounterForFilterInfo++;

                if(currentReporterCounterForFilterInfo> reportCountForRequestFilterInfo){

                    if(isPrintMetric){
                        log.info("report http url: {}, metric info:{}",vmImportUrl, metricBuilder.toString());
                    }

                    currentReporterCounterForFilterInfo= 0;
                    requestFilterMetricInfo();
                }
            }

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

            lableKey= CHARACTER_FILTER.filterCharacters(lableKey);
            if(filterLableValueCharacter){
                lableValue= CHARACTER_FILTER.filterCharacters(lableValue);
            }

            if(i== keyTags.length-1){
                lableBuilder.append(String.format("%s=\"%s\"",lableKey, lableValue));
            }else{
                lableBuilder.append(String.format("%s=\"%s\",",lableKey, lableValue));
            }
        }

        if(groupKeyString.length() >0){
            lableBuilder.append(","+groupKeyString);
        }

        lableBuilder.append(","+ String.format("exported_job=\"%s\"", jobName));

        String lableString= lableBuilder.toString();
        String metricValue = getMetricValue(metric);

        String fullMetricName = getFullMetricName(vmMetricInfo.getName());

        return String.format("%s{%s} %s", fullMetricName, lableString, metricValue);
    }

    private String getFullMetricName(String originMetricName){
        String fullMetricName=SCOPE_PREFIX+CHARACTER_FILTER.filterCharacters(originMetricName) +metricNameSuffix;
        return fullMetricName;
    }



    public String getMetricValue(Metric metric) {
        double result= 0;
        if(metric instanceof Gauge){
            Object value = ((Gauge)metric).getValue();
            if (value == null) {
                log.debug("Gauge {} is null-valued, defaulting to 0.", metric);
                result= 0;
            }else if(value instanceof Boolean) {
                result= ((Boolean) value) ? 1 : 0;
            }else if (value instanceof Double) {
                result= (double) value;
            }else if (value instanceof Number) {
                result= ((Number) value).doubleValue();
            }

        }else if(metric instanceof Counter){
            Counter counter= (Counter) metric;
            result= (double) counter.getCount();
        }else if(metric instanceof Meter){
            Meter meter= (Meter) metric;
            result= meter.getRate();
        }

        return String.valueOf(result);
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

    @VisibleForTesting
    static String replaceInvalidChars(final String input) {
        // Only [a-zA-Z0-9:_] are valid in metric names, any other characters should be sanitized to
        // an underscore.
        return UNALLOWED_CHAR_PATTERN.matcher(input).replaceAll("_");
    }

    private void requestFilterMetricInfo(){
        String curFilterMetricUrl= filterMetricUrl;
        if(curFilterMetricUrl.contains("jobName")){
            curFilterMetricUrl+=jobName;
        }

        String filterMetricJson = HttpClientUtil.postJsonWithParam(curFilterMetricUrl, null, null);
        log.info("request filter url:{},response: {}", curFilterMetricUrl, filterMetricJson);
        if(StringUtils.isNullOrWhitespaceOnly(filterMetricJson)){
            log.error("Failed when request filter http: "+curFilterMetricUrl);
            return;
        }

        try{
            JSONObject requestMetricJo = JSON.parseObject(filterMetricJson);
            if(!requestMetricJo.getBoolean("success")){
                log.error("Failed when request filter http:{}",curFilterMetricUrl);
            }

            JSONArray jaFilterMetrics = requestMetricJo.getJSONArray("result");
            if(jaFilterMetrics.size()==0){
                log.warn("Filter metric is empty, the http:{}", curFilterMetricUrl);
                return;
            }else{
                filterMetricInfo.clearAllMetric();
            }

            for(int i=0; i<jaFilterMetrics.size(); i++){
                JSONObject joFilterMetric = jaFilterMetrics.getJSONObject(i);
                String useType = joFilterMetric.getString("useType");
                String metricName = joFilterMetric.getString("metricName");
                if("INCLUDE_METRIC".equals(useType)){
                    filterMetricInfo.addIncludeMetric(metricName);
                }else if("EXCLUDE_METRIC".equals(useType)){
                    filterMetricInfo.addExcludeMetric(metricName);
                }
            }
            log.info("after request http, filterMetricInfo IncludeMetric :{}, ExcludeMetric:{}"
                    ,filterMetricInfo.getIncludeMetric(), filterMetricInfo.getExcludeMetric());
        }catch (Exception e){
            log.error("Failed when parse filter information with the http:{}, {}",curFilterMetricUrl, e);
        }

    }

}
