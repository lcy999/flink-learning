package com.lcy.flinksql.udf;

/**
 * @author: lcy
 * @date: 2023/11/9
 **/
public class AvgStringAccumulator {
    public double item1;
    public double item2;
    public double item3;
    public double item4;
    public double item5;
    public double item6;
    public double item7;
    public double item8;
    public long   count;



    public AvgStringAccumulator(){}

    public AvgStringAccumulator(double item1, double item2, double item3, double item4, double item5, double item6, double item7, double item8, long count){
        this.item1=item1;
        this.item2=item2;
        this.item3=item3;
        this.item4=item4;
        this.item5=item5;
        this.item6=item6;
        this.item7=item7;
        this.item8=item8;
        this.count=count;
    }
}
