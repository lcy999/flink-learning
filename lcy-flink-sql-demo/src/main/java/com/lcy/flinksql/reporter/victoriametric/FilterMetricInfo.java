package com.lcy.flinksql.reporter.victoriametric;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: lcy
 * @date: 2024/1/31
 **/
public class FilterMetricInfo {
    private List<String> includeMetric;
    private List<String> excludeMetric;

    public FilterMetricInfo(){
        includeMetric=new ArrayList<>();
        excludeMetric=new ArrayList<>();
    }

    public void addIncludeMetric(String metric){
        includeMetric.add(metric);
    }

    public void addExcludeMetric(String metric){
        excludeMetric.add(metric);
    }

    public void clearAllMetric(){
        includeMetric.clear();
        excludeMetric.clear();
    }

    public boolean filterMetric(String metricName){
        if(includeMetric.size()> 0){
            return includeMetric.contains(metricName);
        }else if(excludeMetric.size()> 0){
            return !excludeMetric.contains(metricName);
        }else{
            return true;
        }
    }
}
