package com.lcy.flinksql.statedemo;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2022-04-10
 */
public class UserDefinedSource implements SourceFunction<Jason> {

    @Override
    public void run(SourceContext<Jason> sourceContext) throws Exception {
        Random r = new Random();
        for (int i = 1; i <= 5000; ++i) {
            Jason jason = new Jason();
            jason.setName("lcy-" + r.nextInt(100));
            jason.setAge(1);
            sourceContext.collect(jason);
            if(i%10==0){
                Thread.sleep(1000);
            }
        }
    }

    @Override
    public void cancel() {

    }
}
