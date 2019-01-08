package com.even.rpc;

/**
 * Project Name: Web_App
 * Des: RPC框架通过接口的方式来定义协议。
 * Created by Even on 2018/12/4
 */
public interface NameNodeProtocol {
    /*1，定义versionID，Hadoop的RPC框架需要用到*/
    static final long versionID = 1L;

    /*2，模拟客户端获取指定路径的元数据*/
    String getMetaData(String path);
}
