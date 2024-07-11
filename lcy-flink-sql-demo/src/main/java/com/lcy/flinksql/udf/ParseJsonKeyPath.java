package com.lcy.flinksql.udf;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.lcy.flinksql.tools.BaseUdf;
import com.lcy.flinksql.tools.UdfExceptionHandler;

import java.util.ArrayList;
import java.util.List;

public class ParseJsonKeyPath extends BaseUdf<String> {

    private static UdfExceptionHandler udfExceptionHandler = new UdfExceptionHandler();

    @Override
    public String udfBusinessIdentifier() {
        //公共类-解析json并返回指定的key的路径
        return "common-parse-json-key-path";
    }

    public String eval(String keyName, String jsonContent) {
        ArrayList<Object> datas = Lists.newArrayList(keyName, jsonContent);
        return executeFunc(datas, udfExceptionHandler);
    }

    @Override
    protected String execute(List<Object> inputParams) {
        String keyName= (String) inputParams.get(0);
        String jsonContent= (String) inputParams.get(1);
        List<String> resultPath = findKeyInJson(jsonContent, keyName, Lists.newArrayList());
        if(resultPath== null){
            return "";
        }else{
            return String.join(",", resultPath);
        }
    }

    private List<String> findKeyInJson(Object json, String targetKey, List<String> path) {
        if (json instanceof JSONObject) {
            JSONObject jsonObject = (JSONObject) json;
            for (String key : jsonObject.keySet()) {
                List<String> newPath = new ArrayList<>(path);
                newPath.add(key);
                if(key.equals(targetKey)){
                    return newPath;
                }
                List<String> resultPath = findKeyInJson(jsonObject.get(key), targetKey, newPath);
                if(resultPath != null){
                    return resultPath;
                }
            }
        } else if (json instanceof JSONArray) {
            JSONArray jsonArray = (JSONArray) json;
            for (Object jsonElem : jsonArray) {
                List<String> resultPath = findKeyInJson(jsonElem, targetKey, path);
                if(resultPath != null){
                    return resultPath;
                }
            }
        }
        return null;
    }

    @Override
    protected String checkEmptyData(List<Object> inputs) {
        return "";
    }

    @Override
    protected String handleException(List<Object> inputs, Exception e) {
        return "";
    }
}
