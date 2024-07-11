package com.lcy.flinksql.udf;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: lcy
 * @date: 2023/11/9
 **/
public class ParseJsonKeyListAccumulator {
    public List<String> jsonKeyPaths;
    public boolean isContain;

    public ParseJsonKeyListAccumulator(){
        jsonKeyPaths=new ArrayList<>();
    }

}
