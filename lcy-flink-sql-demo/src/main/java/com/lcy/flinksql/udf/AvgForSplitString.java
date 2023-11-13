package com.lcy.flinksql.udf;

import org.apache.flink.table.functions.AggregateFunction;

/**
 * @author: lcy
 * @date: 2023/11/9
 **/
public class AvgForSplitString extends AggregateFunction<String, AvgStringAccumulator> implements BusinessIdentifier {

    @Override
    public String udfBusinessIdentifier() {
        //电商推荐-实时特征召回-拆分字符串，求平均
        return "e_commerce_recommendation-realtime_feature_recall-avg_for_string";
    }

    @Override
    public String getValue(AvgStringAccumulator accumulator) {
        long count = accumulator.count;

        if(count== 0){
            return "";
        }

        double avgItem1 = Math.round(accumulator.item1 * Math.pow(10, 4)) / (Math.pow(10, 4) * count);
        double avgItem2 = Math.round(accumulator.item2 * Math.pow(10, 4)) / (Math.pow(10, 4) * count);
        double avgItem3 = Math.round(accumulator.item3 * Math.pow(10, 4)) / (Math.pow(10, 4) * count);
        double avgItem4 = Math.round(accumulator.item4 * Math.pow(10, 4)) / (Math.pow(10, 4) * count);
        double avgItem5 = Math.round(accumulator.item5 * Math.pow(10, 4)) / (Math.pow(10, 4) * count);
        double avgItem6 = Math.round(accumulator.item6 * Math.pow(10, 4)) / (Math.pow(10, 4) * count);
        double avgItem7 = Math.round(accumulator.item7 * Math.pow(10, 4)) / (Math.pow(10, 4) * count);
        double avgItem8 = Math.round(accumulator.item8 * Math.pow(10, 4)) / (Math.pow(10, 4) * count);
        
        String resultValue = String.valueOf(avgItem1)
                .concat(",").concat(String.valueOf(avgItem2))
                .concat(",").concat(String.valueOf(avgItem3))
                .concat(",").concat(String.valueOf(avgItem4))
                .concat(",").concat(String.valueOf(avgItem5))
                .concat(",").concat(String.valueOf(avgItem6))
                .concat(",").concat(String.valueOf(avgItem7))
                .concat(",").concat(String.valueOf(avgItem8));

        return resultValue;
    }

    @Override
    public AvgStringAccumulator createAccumulator() {
        return new AvgStringAccumulator();
    }

    public void accumulate(AvgStringAccumulator acc, String iValue) {
        if(iValue==null || iValue.split(",").length!=8){
            System.out.println("The string is empty or does not conform to the format in the average calculation，input:"+iValue);
            return ;
        }

        String[] items = iValue.split(",");
        try {
            for (String item : items) {
                Double.parseDouble(item);
            }
        }catch (Exception e){
            System.out.println("convert string to double which failed in the average calculation，input: "+iValue);
            e.printStackTrace();
            return;
        }

        acc.item1+=Double.parseDouble(items[0]);
        acc.item2+=Double.parseDouble(items[1]);
        acc.item3+=Double.parseDouble(items[2]);
        acc.item4+=Double.parseDouble(items[3]);
        acc.item5+=Double.parseDouble(items[4]);
        acc.item6+=Double.parseDouble(items[5]);
        acc.item7+=Double.parseDouble(items[6]);
        acc.item8+=Double.parseDouble(items[7]);

        acc.count+=1;


    }

    public void merge(AvgStringAccumulator acc, Iterable<AvgStringAccumulator> it) {
        for (AvgStringAccumulator item : it) {
            acc.item1+=item.item1;
            acc.item2+=item.item2;
            acc.item3+=item.item3;
            acc.item4+=item.item4;
            acc.item5+=item.item5;
            acc.item6+=item.item6;
            acc.item7+=item.item7;
            acc.item8+=item.item8;

            acc.count+=item.count;
        }
    }

    public void resetAccumulator(AvgStringAccumulator acc) {
        acc.item1=0;
        acc.item2=0;
        acc.item3=0;
        acc.item4=0;
        acc.item5=0;
        acc.item6=0;
        acc.item7=0;
        acc.item8=0;
        acc.count=0;
    }

    public void retract(AvgStringAccumulator acc, String iValue) { }
    
}
