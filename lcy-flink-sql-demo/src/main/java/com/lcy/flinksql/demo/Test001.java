package com.lcy.flinksql.demo;

import org.apache.flink.table.factories.Factory;

import java.util.LinkedList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * @author: lcy
 * @date: 2023/9/8
 **/
public class Test001 {
    public static void main(String[] args) {
        ClassLoader classLoader = Test001.class.getClassLoader();
        final List<Factory> result = new LinkedList<>();
        ServiceLoader.load(Factory.class, classLoader).iterator().forEachRemaining(result::add);
        System.out.println("size: "+result.size());
        for (Factory item : result) {
            System.out.println(item);
        }
//        System.out.println(result);
    }
}
