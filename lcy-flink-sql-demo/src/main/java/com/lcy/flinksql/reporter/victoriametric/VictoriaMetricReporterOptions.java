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

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.TextElement;

/** Config options for the {@link VictoriaMetricReporter}. */
@Documentation.SuffixOption
public class VictoriaMetricReporterOptions {

    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .noDefaultValue()
                    .withDescription("The VictoriaMetric server host.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .defaultValue(-1)
                    .withDescription("The VictoriaMetric server port.");

    public static final ConfigOption<String> JOB_NAME =
            ConfigOptions.key("jobName")
                    .defaultValue("")
                    .withDescription("The job name under which metrics will be pushed");

    public static final ConfigOption<String> GROUPING_KEY =
            ConfigOptions.key("groupingKey")
                    .defaultValue("")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Specifies the grouping key which is the group and global labels of all metrics."
                                                    + " The label name and value are separated by '=', and labels are separated by ';', e.g., %s."
                                                    + " Please ensure that your grouping key meets the %s.",
                                            TextElement.code("k1=v1;k2=v2"))
                                    .build());

    public static final ConfigOption<String> FILTER_METRIC_URL =
            ConfigOptions.key("filterMetricUrl")
                    .noDefaultValue()
                    .withDescription("Request the Url interface to obtain information on filtering metrics.");

    public static final ConfigOption<Boolean> FILTER_METRIC =
            ConfigOptions.key("filterMetric")
                    .defaultValue(false)
                    .withDescription(
                            "Specifies whether to filter metric");

    public static final ConfigOption<Boolean> RANDOM_JOB_NAME_SUFFIX =
            ConfigOptions.key("randomJobNameSuffix")
                    .defaultValue(true)
                    .withDescription(
                            "Specifies whether a random suffix should be appended to the job name.");

}
