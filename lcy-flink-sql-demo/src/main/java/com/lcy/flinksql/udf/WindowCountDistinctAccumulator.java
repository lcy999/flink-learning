package com.lcy.flinksql.udf;

import com.google.common.hash.BloomFilter;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * @author: lcy
 * @date: 2023/11/9
 **/
public class WindowCountDistinctAccumulator {
    public String stepWindow;
    public String lastTime;
    public Map<String, Tuple2<Long, BloomFilter<CharSequence>>> everyTimeCount;

    public WindowCountDistinctAccumulator(){
        everyTimeCount= new HashMap<>();
    }

}
