package com.even.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * des:
 * author: Even
 * create date:2019/1/4
 */
public class ZookeeperClient {
    public static final int SESSION_TIME_OUT = 3000;
    public ZooKeeper zooKeeper;
    public CountDownLatch countDownLatch = new CountDownLatch(1);
    public static final String ZOO_ROOT_PATH = "/";

    /**
     * 连接zookeeper服务
     *
     * @param hosts zookeeper集群hosts
     * @throws IOException
     * @throws InterruptedException
     */

    public void connect(String hosts) throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(hosts, SESSION_TIME_OUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected)
                    /*递减1，为0，await返回*/
                    countDownLatch.countDown();
            }
        });
        /*等待连接成功*/
        countDownLatch.await();
    }

    /**
     * 创建组
     *
     * @param groupName 组名
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void createGroup(String groupName) throws KeeperException, InterruptedException {
        String path = ZOO_ROOT_PATH + groupName;
        String services = zooKeeper.create(path, "services".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("创建组：" + services);
    }

    /**
     * 关闭连接
     *
     * @throws InterruptedException
     */
    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    /**
     * 组成员注册
     *
     * @param groupName  组名
     * @param memberName 成员名
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void join(String groupName, String memberName) throws KeeperException, InterruptedException {
        String path = ZOO_ROOT_PATH + groupName + ZOO_ROOT_PATH + memberName;
        /*创建一个对所有客户端都可见的临时节点*/
        String s = zooKeeper.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("创建成员成功：" + s);
    }

    /**
     * 显示组成员
     *
     * @param groupName 组名
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void list(String groupName) throws KeeperException, InterruptedException {
        String path = ZOO_ROOT_PATH + groupName;
        List<String> children = zooKeeper.getChildren(path, null);
        if (children.isEmpty())
            System.out.println("此分组没有成员！！");
        for (String child : children) {
            System.out.println(child);
        }
    }

    /**
     * 删除组，delete方法不能递归删除，需要先把组内成员删除
     *
     * @param groupName
     * @throws KeeperException
     * @throws InterruptedException
     */
    public void deleteGroup(String groupName) throws KeeperException, InterruptedException {
        String path = ZOO_ROOT_PATH + groupName;
        List<String> children = zooKeeper.getChildren(path, null);
        for (String child : children) {
            zooKeeper.delete(child, -1);
        }
        zooKeeper.delete(path, -1);
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZookeeperClient zookeeperClient = new ZookeeperClient();
        zookeeperClient.connect("hd-even-01:2181,hd-even-02:2181,hd-even-03:2181");
        /*创建组*/
        zookeeperClient.createGroup("services");
        /*创建组成员*/
        zookeeperClient.join("services", "service1");
        /*显示组成员*/
        zookeeperClient.list("services");
        /*删除组成员*/
        zookeeperClient.deleteGroup("services");
        Thread.sleep(Long.MAX_VALUE);
        zookeeperClient.close();
    }
}
