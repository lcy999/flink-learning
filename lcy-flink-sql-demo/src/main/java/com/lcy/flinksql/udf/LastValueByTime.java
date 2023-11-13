package com.lcy.flinksql.udf;

import org.apache.flink.table.functions.AggregateFunction;

import java.sql.Timestamp;

/**
 * @author: lcy
 * @date: 2023/11/9
 **/
public class LastValueByTime extends AggregateFunction<String, LastValueAccumulator> implements BusinessIdentifier {

    @Override
    public String udfBusinessIdentifier() {
        //公共类-根据时间获取最后的值
        return "common-get_last_value_by_time";
    }

    @Override
    public String getValue(LastValueAccumulator accumulator) {
        return accumulator.value;
    }

    @Override
    public LastValueAccumulator createAccumulator() {
        return new LastValueAccumulator();
    }

    public void accumulate(LastValueAccumulator acc, String iValue, Timestamp inputTime) {
        if(inputTime== null){
            return;
        }

        if(iValue== null){
            return;
        }

        if(acc.inputTime==null || acc.inputTime.before(inputTime)){
            acc.inputTime=inputTime;
            acc.value= iValue;
        }
    }

    public void merge(LastValueAccumulator acc, Iterable<LastValueAccumulator> it) {
        for (LastValueAccumulator item : it) {
            accumulate(acc, item.value, item.inputTime);
        }
    }

    public void resetAccumulator(LastValueAccumulator acc) {
        acc.value = null;
        acc.inputTime = null;
    }

    public void retract(LastValueAccumulator acc, String iValue, Timestamp inputTime) { }

}
