package com.lcy.flinksql.udf;

import org.apache.flink.table.functions.TableFunction;

/**
 * @author: lcy
 * @date: 2024/3/21
 **/
public class UserProfileTableFunction extends TableFunction<Integer> {

    public void eval(Integer userId) {
        // 自定义输出逻辑
        if (userId <= 5) {
            // 一行转 1 行
            collect(1);
        } else {
            // 一行转 3 行
            collect(1);
            collect(2);
            collect(3);
        }
    }

}
