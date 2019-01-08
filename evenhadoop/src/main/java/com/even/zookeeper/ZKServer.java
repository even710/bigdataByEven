package com.even.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.CountDownLatch;

/**
 * Project Name: Web_App
 * Des: 服务器注册消息
 * Created by Even on 2019/1/7
 */
public class ZKServer {
    public final String HOSTS = "hd-even-01:2181,hd-even-02:2181,hd-even-03:2181";
    public final int SESSION_TIME_OUT = 3000;
    public CountDownLatch countDownLatch = new CountDownLatch(1);
    public ZooKeeper zookeeper;
    public Charset CHARSET = Charset.forName("UTF-8");

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZKServer zkServer = new ZKServer();
        zkServer.connect();
        zkServer.createGroup("services");
        zkServer.join("services", "service2");
        Thread.sleep(Long.MAX_VALUE);
    }

    public void connect() throws IOException, InterruptedException {
        zookeeper = new ZooKeeper(HOSTS, SESSION_TIME_OUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected)
                    countDownLatch.countDown();
            }
        });
        countDownLatch.await();
    }

    public void createGroup(String groupName) throws KeeperException, InterruptedException {
        Stat stat = zookeeper.exists("/" + groupName, null);
        if (null == stat) {
            String s = zookeeper.create("/" + groupName, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.printf("Create znode %s success\n", s);
        }
    }

    public void join(String groupName, String memberName) throws KeeperException, InterruptedException {
        /*此时创建一个临时带序号的子节点*/
        String s = zookeeper.create("/" + groupName + "/" + "server", memberName.getBytes(CHARSET), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.printf("server %s  上线了\n", s);
    }
}
