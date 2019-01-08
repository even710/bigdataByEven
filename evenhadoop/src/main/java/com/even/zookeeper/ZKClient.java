package com.even.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Project Name: Web_App
 * Des: 监听服务器上下线
 * Created by Even on 2019/1/7
 */
public class ZKClient implements Watcher {
    /*重试次数*/
    private final int MAX_RETRIES = 10;
    /*重试时间差*/
    private final int RETRY_TIME_OUT = 30;

    public final String HOSTS = "hd-even-01:2181,hd-even-02:2181,hd-even-03:2181";
    public final int SESSION_TIME_OUT = 3000;
    /*计数器，用于确保zookeeper已连接成功*/
    public CountDownLatch countDownLatch = new CountDownLatch(1);
    public ZooKeeper zookeeper;
    public Charset CHARSET = Charset.forName("UTF-8");
    /*当前服务器列表，当发生上下线时，会更新*/
    Set<String> serverList = new HashSet<>();

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        while (true) {
            ZKClient zkClient = new ZKClient();
            zkClient.connect("services");
            try {
                zkClient.listen("services");
            } catch (KeeperException.SessionExpiredException e) {
                break;
            } catch (KeeperException e) {
                throw e;
            }
        }
    }
    /**
     * 连接zookeeper服务，获取指定znode的子节点，并保存下来
     * @param group
     * @throws IOException
     * @throws InterruptedException
     */
    public void connect(final String group) throws IOException, InterruptedException {
        zookeeper = new ZooKeeper(HOSTS, SESSION_TIME_OUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    countDownLatch.countDown();
                    try {
                        List<String> children = zookeeper.getChildren("/" + group, null);
                        for (String child : children) {
                            byte[] data = zookeeper.getData("/" + group + "/" + child, null, null);
                            serverList.add(new String(data, CHARSET));
                        }
                        System.out.printf("server list:%s\n", serverList);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        countDownLatch.await();
    }
    /**
     * 监听指定znode下的子znode变化，如果发生变化，就打印出来
     * @param groupName
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void listen(String groupName) throws InterruptedException, KeeperException {
        int retry = 0;
        while (true) {
            try {
                Stat stat = zookeeper.exists("/" + groupName, null);
                if (null == stat) {
                    zookeeper.create("/" + groupName, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                List<String> children = zookeeper.getChildren("/" + groupName, this);
                /*获取发生改变后的服务器列表*/
                Set<String> currentServerList = new HashSet<>();
                Set<String> result = new HashSet<>();
                for (String child : children) {
                    byte[] data = zookeeper.getData("/" + groupName + "/" + child, true, null);
                    currentServerList.add(new String(data, CHARSET));
                }
                /*求出下线的服务器列表*/
                result.addAll(serverList);
                result.removeAll(currentServerList);
                if (result.size() != 0)
                    System.out.printf("server %s 下线了\n", result);
                /*求出上线的服务器列表*/
                result.clear();
                result.addAll(currentServerList);
                result.removeAll(serverList);
                if (result.size() != 0)
                    System.out.printf("server %s 上线了\n", result);
                /*更新当前服务器列表*/
                serverList = currentServerList;
            } catch (KeeperException.SessionExpiredException e) {
                e.printStackTrace();
                throw e;
            } catch (KeeperException e) {
                /*重试次数*/
                if (retry++ == MAX_RETRIES)
                    throw e;
                /*重试等待时间*/
                Thread.sleep(RETRY_TIME_OUT);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
    /**
     * 指定事件类型为子节点更新
     * @param event
     */
    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeChildrenChanged) {
            try {
                /*发生子节点更新时*/
                listen("services");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            }
        }
    }
}
