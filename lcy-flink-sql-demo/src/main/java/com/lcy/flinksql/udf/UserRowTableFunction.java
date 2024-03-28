package com.lcy.flinksql.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author: lcy
 * @date: 2024/3/21
 **/
@FunctionHint(output = @DataTypeHint("ROW<user_id INT, user_name STRING>"))
public class UserRowTableFunction extends TableFunction<Row> {

    public void eval(Integer userId) {
        // 自定义输出逻辑
        if (userId <= 5) {
            // 一行转 1 行
            collect(Row.of(userId, "username_001"));
        } else {
            // 一行转 3 行

            collect(Row.of(userId, "username_002"));
            collect(Row.of(userId, "username_003"));
            collect(Row.of(userId, "username_004"));
        }
    }

}
