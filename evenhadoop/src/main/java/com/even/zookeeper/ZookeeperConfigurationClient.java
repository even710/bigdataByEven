package com.even.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * des:
 * author: Even
 * create date:2019/1/6
 */
public class ZookeeperConfigurationClient extends ZookeeperClient {
    private static final Charset CHARSET = Charset.forName("UTF-8");
    private Random random = new Random();
    private final String PATH = "/data";

    private void write(String path, String value) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(path, false);
        if (null == stat)
            zooKeeper.create(path, value.getBytes(CHARSET), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        else
            zooKeeper.setData(path, value.getBytes(CHARSET), stat.getVersion());
    }

    private final int MAX_RETRIES = 10;
    private final int RETRY_TIME_OUT = 30;

    public void run() throws KeeperException, InterruptedException {
        int retries = 0;
        while (true) {
            String value = random.nextInt(100) + "";
            try {
                write(PATH, value);
                System.out.printf("Set %s to %s\n", PATH, value);

            } catch (KeeperException.SessionExpiredException e) {
                e.printStackTrace();
                throw e;
            } catch (KeeperException e) {
                e.printStackTrace();
                if (retries++ == MAX_RETRIES)
                    throw e;
                TimeUnit.SECONDS.sleep(RETRY_TIME_OUT);
            }

        }
    }

    public String read(String path, Watcher watcher) throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData(path, watcher, null);
        return new String(data, CHARSET);

    }

    public static void main(String[] args) {
        while (true) {
            ZookeeperConfigurationClient zookeeperConfigurationClient = new ZookeeperConfigurationClient();
            try {
                zookeeperConfigurationClient.connect("hd-even-01:2181,hd-even-02:2181,hd-even-03:2181");
                try {
                    zookeeperConfigurationClient.run();
                } catch (KeeperException.SessionExpiredException e) {
                    /*代表session超时，继续循环启动新的session*/
                    e.printStackTrace();
                } catch (KeeperException e) {
                    /*表示已retry过，所以可以退出*/
                    e.printStackTrace();
                    break;
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
                break;
            }
        }
    }
}
