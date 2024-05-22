package com.lcy.flinksql.udf;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.flink.udf.beans.ParseJsonKeyListAccumulator;
import org.apache.flink.table.functions.AggregateFunction;

import java.sql.Timestamp;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author: lcy
 * @date: 2023/11/9
 **/
public class ParseJsonKeyListPath extends AggregateFunction<String, ParseJsonKeyListAccumulator> implements BusinessIdentifier {

    @Override
    public String udfBusinessIdentifier() {
        //公共类-公共类-解析json并返回key的路径
        return "common-parse-json-key-list-path";
    }

    @Override
    public String getValue(ParseJsonKeyListAccumulator accumulator) {
        if (accumulator.isContain || accumulator.jsonKeyPaths.size() == 0) {
            return null;
        }

        List<String> keys = accumulator.jsonKeyPaths;
        JSONObject jsonObject = new JSONObject();
        for (String key : keys) {
            addKeys(jsonObject, key.split("->"));
        }
        accumulator.isContain=true;

        return jsonObject.toJSONString();
    }

    @Override
    public ParseJsonKeyListAccumulator createAccumulator() {
        return new ParseJsonKeyListAccumulator();
    }

    public void accumulate(ParseJsonKeyListAccumulator acc, String iValue) {

        if (iValue == null) {
            return;
        }

        JSONObject originalJson = JSON.parseObject(iValue);
        List<String> keys = new ArrayList<>();
//        findAllKeyInJson(originalJson, "", keys);

        LinkedBlockingDeque<AbstractMap.SimpleEntry<String, JSONObject>> queue = new LinkedBlockingDeque<>();
        queue.add(new AbstractMap.SimpleEntry<>("", originalJson));

        while(!queue.isEmpty()) {
            AbstractMap.SimpleEntry<String, JSONObject> currentEntry = queue.poll();
            String parentKey = currentEntry.getKey();
            JSONObject currentValue = currentEntry.getValue();

            for(String key: currentValue.keySet()) {
                String newKey = parentKey.isEmpty() ? key : parentKey + "->" + key;
                Object value = currentValue.get(key);
                if (value instanceof JSONObject && ((JSONObject) value).size()>0 ) {
                    // 如果value是一个JSONObject，则将其加入队列进行下一轮处理
                    queue.add(new AbstractMap.SimpleEntry<>(newKey, (JSONObject) value));
                }else if (value instanceof JSONArray && ((JSONArray) value).size()>0) {
                    JSONArray jsonArray = (JSONArray) value;
                    for (Object jsonElem : jsonArray) {
                        if (jsonElem instanceof JSONObject) {
                            // 如果value是一个JSONObject，则将其加入队列进行下一轮处理
                            queue.add(new AbstractMap.SimpleEntry<>(newKey, (JSONObject) jsonElem));
                        }
                    }
                } else {
                    // 如果value不是一个JSONObject，加入到结果List中
                    keys.add(newKey);
                }
            }
        }


        for (String key : keys) {
            if (!acc.jsonKeyPaths.contains(key)) {
                acc.jsonKeyPaths.add(key);
                acc.isContain= false;
            }
        }
    }

    public void merge(ParseJsonKeyListAccumulator acc, Iterable<ParseJsonKeyListAccumulator> it) {

        for (ParseJsonKeyListAccumulator item : it) {
            for (String jsonKeyPath : item.jsonKeyPaths) {
                if (!acc.jsonKeyPaths.contains(jsonKeyPath)) {
                    acc.jsonKeyPaths.add(jsonKeyPath);
                    acc.isContain = false;
                }
            }
        }

    }

    public void resetAccumulator(ParseJsonKeyListAccumulator acc) {
        acc.isContain = true;
        acc.jsonKeyPaths = new ArrayList<>();
    }

    public void retract(ParseJsonKeyListAccumulator acc, String iValue, Timestamp inputTime) {
    }


    public static void addKeys(JSONObject jsonObject, String[] keys) {
        for (int i = 0; i < keys.length; i++) {
            String key = keys[i];
            if (i == keys.length - 1) {
                // 如果是最后一个key，添加一个空的JSONObject
                jsonObject.put(key, new JSONObject());
            } else {
                // 如果不是最后一个key，需要创建或获取下一级的JSONObject
                JSONObject nextJsonObject = jsonObject.containsKey(key) ? jsonObject.getJSONObject(key) : new JSONObject();
                jsonObject.put(key, nextJsonObject);
                jsonObject = nextJsonObject;
            }
        }
    }

    private List<String> findAllKeyInJson(Object json, String targetKey, List<String> path) {
        if (json instanceof JSONObject) {
            JSONObject jsonObject = (JSONObject) json;
            for (String key : jsonObject.keySet()) {
                Object subObject = jsonObject.get(key);
                String targetKeyNew = targetKey + "->" + key;
                if (targetKey.length() == 0) {
                    targetKeyNew = key;
                }
                if (subObject instanceof JSONObject || subObject instanceof JSONArray) {
                    List<String> resultPath = findAllKeyInJson(subObject, targetKeyNew, path);
                    if (resultPath != null) {
                        return resultPath;
                    }
                } else {
                    if (!path.contains(targetKey)) {
                        path.add(targetKeyNew);
                    }
                }
            }
        } else if (json instanceof JSONArray) {
            JSONArray jsonArray = (JSONArray) json;
            for (Object jsonElem : jsonArray) {
                List<String> resultPath = findAllKeyInJson(jsonElem, targetKey, path);
                if (resultPath != null) {
                    return resultPath;
                }
            }
        }
        return null;
    }

}
