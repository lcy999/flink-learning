package com.lcy.flinksql.udf;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.AggregateFunction;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;

/**
 * @author: lcy
 * @date: 2023/11/9
 **/
public class WindowCountDistinct extends AggregateFunction<Long, WindowCountDistinctAccumulator> implements BusinessIdentifier {

    @Override
    public String udfBusinessIdentifier() {
        //公共类-窗口去重计数
        return "common-window-count-distinct";
    }

    @Override
    public Long getValue(WindowCountDistinctAccumulator accumulator) {
        int retainWindowNum = getRetainWindowNum(accumulator.stepWindow);
        List<String> windowTimeList = getMinusWindowTimeList(accumulator.stepWindow, retainWindowNum, accumulator.lastTime);
        if(windowTimeList.size()== 0){
            System.out.println("warn msg, common-window-count-distinct get value, bug windowTimeList is empty");
            return -1L;
        }

        String curWindow = windowTimeList.get(0);
        Tuple2<Long, BloomFilter<CharSequence>> curBloomFilterValue = accumulator.everyTimeCount.get(curWindow);
        if(curBloomFilterValue== null){
            System.out.println("warn msg, common-window-count-distinct get value, the window get failed:"+curWindow);
            return -1L;
        }
        Long curResult= curBloomFilterValue.f0;

        //清理没有用的window
        int clearWindowNum = getClearWindowNum(accumulator.stepWindow);
        if(accumulator.everyTimeCount.size()> retainWindowNum+clearWindowNum){
            String minWindow = windowTimeList.get(windowTimeList.size()-1);
            Iterator<String> iteratorWindow = accumulator.everyTimeCount.keySet().iterator();
            System.out.println(String.format("prepare clarn window, all size:%s, retainWindowNum:%s, clearWindowNum:%s "
                    ,accumulator.everyTimeCount.size(), retainWindowNum, clearWindowNum));
            while(iteratorWindow.hasNext()){
                String item = iteratorWindow.next();
                if(minWindow.compareToIgnoreCase(item)>0){
                    System.out.println("clear window: "+ item);
                    accumulator.everyTimeCount.remove(item);
                }
            }

        }


        return curResult;

    }

    private int getWindowStep(String stepWindow) {
        return Integer.parseInt(stepWindow.split(" ")[0]);
    }

    private String getWindowStepUnit(String stepWindow) {
        return stepWindow.split(" ")[1];
    }

    private int getRetainWindowNum(String stepWindow){
        int windowStep = getWindowStep(stepWindow);
        String windowStepUnit = getWindowStepUnit(stepWindow);
        int result= 0;
        if (windowStepUnit.startsWith("day")) {
            result= windowStep>10? (windowStep+3): windowStep/2+windowStep;
        } else if (windowStepUnit.startsWith("hour")) {
            result=windowStep/2+windowStep;
        } else if (windowStepUnit.startsWith("minute")) {
            result= windowStep*2+10;
        }

        return result;
    }

    private int getClearWindowNum(String stepWindow){
        String windowStepUnit = getWindowStepUnit(stepWindow);
        int result= 0;
        if (windowStepUnit.startsWith("day")) {
            result= 1;
        } else if (windowStepUnit.startsWith("hour")) {
            result=2;
        } else if (windowStepUnit.startsWith("minute")) {
            result= 60;
        }

        return result;
    }

    private List<String> getPlusWindowTimeList(String stepWindow, Integer needWindowNum, String lastTime) {
        List<String> windowTimeList=new ArrayList<>();
        String type = getWindowStepUnit(stepWindow);
        int num = getWindowStep(stepWindow);
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.CHINA);
        DateTimeFormatter hourFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH", Locale.CHINA);
        DateTimeFormatter minuteFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm", Locale.CHINA);
        num = num <= needWindowNum ? num : needWindowNum ;
        LocalDateTime curDateTime = LocalDateTime.parse(lastTime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.CHINA));
        if (type.startsWith("day")) {
            for (int i = 0; i < num; ++i) {
                String timeFormat = curDateTime.plusDays(i).format(dateFormatter);
                windowTimeList.add(timeFormat);
            }
        } else if (type.startsWith("hour")) {
            for (int i = 0; i < num; ++i) {
                String timeFormat = curDateTime.plusHours(i).format(hourFormatter);
                windowTimeList.add(timeFormat);
            }
        } else if (type.startsWith("minute")) {
            for (int i = 0; i < num; ++i) {
                String timeFormat = curDateTime.plusMinutes(i).format(minuteFormatter);
                windowTimeList.add(timeFormat);
            }
        }else{
            throw new RuntimeException("window step param failed, such as '3 day','3 hour','3 minute'");
        }

        return windowTimeList;
    }

    private List<String> getMinusWindowTimeList(String stepWindow, Integer needWindowNum, String lastTime) {
        List<String> windowTimeList=new ArrayList<>();
        String type = getWindowStepUnit(stepWindow);
        int num = getWindowStep(stepWindow);
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.CHINA);
        DateTimeFormatter hourFormatter = DateTimeFormatter.ofPattern("yyyyMMddHH", Locale.CHINA);
        DateTimeFormatter minuteFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmm", Locale.CHINA);
        num = num <= needWindowNum ? num : needWindowNum ;
        LocalDateTime curDateTime = LocalDateTime.parse(lastTime, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.CHINA));
        if (type.startsWith("day")) {
            for (int i = 0; i < num; ++i) {
                String timeFormat = curDateTime.minusDays(i).format(dateFormatter);
                windowTimeList.add(timeFormat);
            }
        } else if (type.startsWith("hour")) {
            for (int i = 0; i < num; ++i) {
                String timeFormat = curDateTime.plusHours(i).format(hourFormatter);
                windowTimeList.add(timeFormat);
            }
        } else if (type.startsWith("minute")) {
            for (int i = 0; i < num; ++i) {
                String timeFormat = curDateTime.plusMinutes(i).format(minuteFormatter);
                windowTimeList.add(timeFormat);
            }
        }else{
            throw new RuntimeException("window step param failed, such as '3 day','3 hour','3 minute'");
        }

        return windowTimeList;
    }


    @Override
    public WindowCountDistinctAccumulator createAccumulator() {
        return new WindowCountDistinctAccumulator();
    }

    public void accumulate(WindowCountDistinctAccumulator acc, String stepWindow, String lastTime, String value) {
        if (stepWindow == null) {
            return;
        }

        if (lastTime == null) {
            return;
        }

        if(acc.stepWindow== null){
            acc.stepWindow= stepWindow;
        }

        if(acc.lastTime.compareToIgnoreCase(lastTime)< 0){
            acc.lastTime= lastTime;
        }

        List<String> plusWindowTimeList = getPlusWindowTimeList(stepWindow, getWindowStep(stepWindow), lastTime);
        for (String item : plusWindowTimeList) {
            if(acc.everyTimeCount.containsKey(item)){
                Tuple2<Long, BloomFilter<CharSequence>> longBloomFilter = acc.everyTimeCount.get(item);
                BloomFilter<CharSequence> bloomFilter = longBloomFilter.f1;
                if(!bloomFilter.mightContain(value)){
                    bloomFilter.put(value);
                    longBloomFilter.f0= longBloomFilter.f0+1;
                }
            }else{
                long capacity = 100000L;
                // 错误比率
                double errorRate = 0.0001;
                //创建BloomFilter对象，需要传入Funnel对象，预估的元素个数，错误率
                BloomFilter<CharSequence> filter = BloomFilter.create(Funnels.unencodedCharsFunnel(), capacity, errorRate);
                acc.everyTimeCount.put(item, Tuple2.of(1L,filter));
            }
        }


    }

    public void merge(WindowCountDistinctAccumulator acc, Iterable<WindowCountDistinctAccumulator> it) {
        /*for (WindowCountDistinctAccumulator item : it) {
            accumulate(acc, item.value, item.inputTime);
        }*/
    }

    public void resetAccumulator(WindowCountDistinctAccumulator acc) {
        acc= new WindowCountDistinctAccumulator();
    }

    public void retract(LastValueAccumulator acc, String iValue, Timestamp inputTime) {
    }

}
