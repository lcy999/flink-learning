package com.lcy.flinksql.tools;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

public abstract class BaseUdf<T> extends ScalarFunction implements BusinessIdentifier{

    protected abstract T execute(List<Object> inputParams);

    protected T checkEmptyData(List<Object> inputs){
        return null;
    }

    protected T handleException(List<Object> inputs, Exception e){
        return null;
    }

    protected T executeFunc(List<Object> inputs, UdfExceptionHandler exceptionHandler) {
        List<Boolean> exceptions = new ArrayList<>();
        for (Object input : inputs) {
            exceptions.add(false);
        }
        return executeFunc(inputs, exceptions, exceptionHandler);
    }

    protected T executeFunc(List<Object> inputs,List<Boolean> excetpions, UdfExceptionHandler exceptionHandler) {
        boolean isCheckPass = checkParam(inputs, excetpions);
        if (!isCheckPass) {
            return checkEmptyData(inputs);
        }
        try {
            List<Object> datas = new ArrayList<>();
            for (Object input : inputs) {
                datas.add(input);
            }
            return execute(datas);
        } catch (Exception e) {
            exceptionHandler.handleUdfException(e, inputs.get(0).toString());
            return handleException(inputs, e);
        }
    }



    protected boolean checkParam(List<Object> params) {
        IdentityHashMap<Boolean, Object> paramExceptionMap = new IdentityHashMap<>();
        for (Object param : params) {
            paramExceptionMap.put(new Boolean(false), param);
        }

        return checkParam(paramExceptionMap);
    }

    protected boolean checkParam(List<Object> datas,List<Boolean> excetpions) {
        if (datas.size() != excetpions.size()) {
            throw new RuntimeException("please check param");
        }

        IdentityHashMap<Boolean, Object> paramExceptionMap = new IdentityHashMap<>();
        for (int i = 0; i < datas.size(); i++) {
            paramExceptionMap.put(new Boolean(excetpions.get(i)), datas.get(i));
        }

        return checkParam(paramExceptionMap);
    }

    /**
     * @param params Boolean, true表示如果该参数为空，要抛出异常
     * @return
     */
    protected boolean checkParam(Map<Boolean, Object> params) {
        for (Map.Entry<Boolean, Object> item : params.entrySet()) {
            Object data = item.getValue();
            Boolean isException = item.getKey();
            if (data != null && String.valueOf(data).length() > 0) {
                continue;
            }
            if (isException) {
                throw new RuntimeException(String.format("input param is empty, please check"));
            } else {
                return false;
            }
        }
        return true;
    }


}
