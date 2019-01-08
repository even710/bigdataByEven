package com.even.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * Project Name: Web_App
 * Des: RPC服务端代码
 * Created by Even on 2018/12/4
 */
public class RPCServer {
    public static void main(String[] args) throws IOException {
        /*1,构造RPC框架，指定默认的配置*/
        RPC.Builder rpc = new RPC.Builder(new Configuration());
        /*2，绑定Host*/
        rpc.setBindAddress("localhost");
        /*3，绑定端口*/
        rpc.setPort(9999);
        /*4，设置协议*/
        rpc.setProtocol(NameNodeProtocol.class);
        /*5，设置协议实现类*/
        rpc.setInstance(new NameNodeProtocolImpl());
        /*6，启动服务*/
        RPC.Server rpcServer = rpc.build();
        rpcServer.start();
    }
}
