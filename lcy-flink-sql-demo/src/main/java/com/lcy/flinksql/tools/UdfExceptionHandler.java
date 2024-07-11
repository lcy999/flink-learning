package com.lcy.flinksql.tools;
import java.util.ArrayList;
import java.util.List;

public class UdfExceptionHandler {
    private List<String> errors = new ArrayList<>();
    private long errorCount=0;
    private long errorTerminalCount=0;
    public static final int PRINT_ERROR_COUNT = 3000;
    public static final int TERMINAL_ERROR_COUNT = 30;

    public  void handleUdfException(Exception e, String data) {
        errorCount++;
        String errorName = e.getClass().getName();
        if (!errors.contains(errorName)) {
            errors.add(errorName);
            e.printStackTrace();
            System.out.println("error data: "+data);
        }

        if (errorCount % PRINT_ERROR_COUNT == 0) {
            System.out.println("udf error count:"+errorCount);
            e.printStackTrace();
        }

        if (errorTerminalCount > TERMINAL_ERROR_COUNT) {
            throw new RuntimeException("get key exception from service, please check third server or env");
        }


    }

}
