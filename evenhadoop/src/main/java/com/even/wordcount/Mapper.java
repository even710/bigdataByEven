package com.even.wordcount;

/**
 * Project Name: Web_App
 * Des:接口设计，map阶段处理数据
 * Created by Even on 2018/12/4
 */
public interface Mapper {
    /*把linde拆分并封装到context中*/
    void map(String line, Context context);

}
