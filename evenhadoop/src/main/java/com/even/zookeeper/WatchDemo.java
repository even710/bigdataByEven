package com.even.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Arrays;

/**
 * Project Name: Web_App
 * Des: 监听单节点内容
 * Created by Even on 2019/1/4
 */
public class WatchDemo {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZooKeeper zooKeeper = new ZooKeeper("hd-even-01:2181,hd-even-02:2181,hd-even-03:2181", 3000, new Watcher() {
            /*接收创建Zookeeper时的回调*/
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("根目录==>监听路径为：" + watchedEvent.getPath());
                System.out.println("根目录==>监听类型为：" + watchedEvent.getType());
                System.out.println("根目录==>数据被修改了");
            }
        });

        byte[] data = zooKeeper.getData("/even", new Watcher() {
            /*监听的具体内容，只会监听一次*/
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("监听路径为：" + watchedEvent.getPath());
                System.out.println("监听类型为：" + watchedEvent.getType());
                System.out.println("数据被修改了");
            }
        }, null);
        System.out.println(Arrays.toString(data));
        Thread.sleep(Long.MAX_VALUE);
    }
}
