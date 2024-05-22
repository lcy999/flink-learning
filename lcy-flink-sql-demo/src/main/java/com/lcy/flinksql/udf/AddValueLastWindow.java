package com.lcy.flinksql.udf;

import org.apache.flink.table.functions.AggregateFunction;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: lcy
 * @date: 2023/11/9
 **/
public class AddValueLastWindow extends AggregateFunction<Long, Map<String,Long>> implements BusinessIdentifier {

    @Override
    public String udfBusinessIdentifier() {
        //公共类-加上个窗口的统计值
        return "common-add-value-last-window";
    }

    @Override
    public Long getValue(Map<String,Long> accumulator) {
//        System.out.println("map value: "+accumulator);
        int size = accumulator.size();
        if(size== 0){
            return -1L;
        }else if(size==1){
            return (Long)accumulator.values().toArray()[0];
        }

        Object[] keys = accumulator.keySet().toArray();
        Arrays.sort(keys);
        Long result= accumulator.get(keys[size-1]);
        for(int i=size-2;i>=0; i--){
            if(i==size-2){
                result+= accumulator.get(keys[i]);
            }else{
                if(result==200){
                    System.out.println("map value: "+accumulator+"   remove: "+keys[i]);
                }
                accumulator.remove(keys[i]);
            }
        }

        return result;

    }

    @Override
    public Map<String,Long> createAccumulator() {
        return new HashMap<>();
    }

    public void accumulate(Map<String,Long> acc, Long iValue, String windowTime) {
        acc.put(windowTime,iValue);

    }

    public void merge(Map<String,Long> acc, Iterable<Map<String,Long>> it) {
        for (Map<String,Long> item : it) {
            for(Map.Entry<String,Long> subItem: item.entrySet()){
                String windowKey = subItem.getKey();
                Long iValue = subItem.getValue();
                Long value = acc.get(windowKey);
                if(value== null){
                    acc.put(windowKey, iValue);
                }else{
                    acc.put(windowKey, iValue+value);
                }
            }
        }
    }

    public void resetAccumulator(Map<String,Long> acc) {
        acc.clear();
    }

    public void retract(LastValueAccumulator acc, String iValue, Timestamp inputTime) { }

}
