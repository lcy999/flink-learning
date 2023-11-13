package com.lcy.flinksql.udf;

import java.sql.Timestamp;

/**
 * @author: lcy
 * @date: 2023/11/9
 **/
public class LastValueAccumulator {
    public String value;
    public Timestamp inputTime;

    public LastValueAccumulator(){

    }

    public LastValueAccumulator(String value, Timestamp inputTime){
        this.value= value;
        this.inputTime= inputTime;
    }
}
