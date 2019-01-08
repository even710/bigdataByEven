package com.even.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Project Name: Web_App
 * Des: RPC客户端
 * Created by Even on 2018/12/4
 */
public class RPCClient {
    public static void main(String[] args) throws IOException {
        /*1，通过代理模式获取协议*/
        NameNodeProtocol rpcClient = RPC.getProxy(NameNodeProtocol.class, NameNodeProtocol.versionID, new
                InetSocketAddress("localhost", 9999), new Configuration());
        /*2，调用协议方法，发送请求*/
        String meta = rpcClient.getMetaData("/even");
        /*3，拿到元数据*/
        System.out.println("meta:" + meta);
    }
}
