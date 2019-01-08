package com.even.wordcount;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Project Name: Web_App
 * Des: 数据传输的类，用于封装数据
 * Created by Even on 2018/12/4
 */
public class Context {

        private HashMap<Object, Object> contextMap = new HashMap<>();
//    private ArrayList<String> contextMap = new ArrayList<>();

    /*读数据*/
    public Object get(Object key) {
        return contextMap.get(key);
    }

    /*写数据*/
    public void write(Object key, Object value) {
//        contextMap.add(key.toString());
        contextMap.put(key, value);
    }

    /*获取Map全部数据*/
    public HashMap<Object, Object> getContextMap() {
        return contextMap;
    }
//    public ArrayList<String> getContextMap() {
//        return contextMap;
//    }
}
