package com.lcy.flinksql.reporter.victoriametric;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.SimpleCounter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.lcy.flinksql.reporter.victoriametric.VictoriaMetricReporterOptions.*;

public class VictoriaMetricReporterTest {

    private VictoriaMetricReporter reporter;

    @Before
    public void init(){
        reporter = new VictoriaMetricReporter();
        MetricConfig config=new MetricConfig();
        config.put(HOST.key(),"127.0.0.1");
        config.put(PORT.key(), "8459");
        config.put(GROUPING_KEY.key(),"k1=v1;k2=v2");
        config.put(JOB_NAME.key(),"00037089_202401311039_");
        reporter.open(config);

    }

    @Test
    public void testBuildVMPushMetric(){
        Counter testCounter = new SimpleCounter();
        testCounter.inc(7);

        String metricName="flink_taskmanager_test01";
        Map<String, String> tags=new HashMap<>();
        tags.put("job_name","00037089");
        VictoriaMetricInfo metricInfo=new VictoriaMetricInfo(metricName,tags);

        String metricContent = reporter.buildVMPushMetric(testCounter, metricInfo);
        System.out.println(metricContent);
        Assert.assertTrue(metricContent.contains("flink_taskmanager_test01{job_name=\"00037089\",k1=\"v1\",k2=\"v2\",job=\"00037089_202401311039"));
    }

    @Test
    public void testParseGroupingKey() {
        Map<String, String> groupingKey =
                reporter.parseGroupingKey("k1=v1;k2=v2");
        Assert.assertNotNull(groupingKey);
        Assert.assertEquals("v1", groupingKey.get("k1"));
        Assert.assertEquals("v2", groupingKey.get("k2"));
    }

}