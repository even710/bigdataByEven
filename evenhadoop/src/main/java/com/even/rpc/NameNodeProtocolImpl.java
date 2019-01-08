package com.even.rpc;

/**
 * Project Name: Web_App
 * Des:协议的实现类
 * Created by Even on 2018/12/4
 */
public class NameNodeProtocolImpl implements NameNodeProtocol {
    @Override
    public String getMetaData(String path) {
        /*返回元数据，路径+副本数+块信息*/
        return path+":3 - {BLK_1,BLK_4,BLK_5}";
    }
}
