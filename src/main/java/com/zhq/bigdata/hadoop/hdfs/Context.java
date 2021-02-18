package com.zhq.bigdata.hadoop.hdfs;

import java.util.HashMap;
import java.util.Map;

public class Context {
    private Map<Object,Object> cacheMap = new HashMap<>();

    public Map<Object,Object> getCacheMap(){
        return cacheMap;
    }

    public void write(Object key,Object value){
        cacheMap.put(key,value);
    }

    public Object get(Object key){
        return cacheMap.get(key);
    }
}
