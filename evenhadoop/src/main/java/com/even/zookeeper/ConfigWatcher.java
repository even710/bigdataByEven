package com.even.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;

/**
 * des:
 * author: Even
 * create date:2019/1/6
 */
public class ConfigWatcher implements Watcher {

    private final String PATH = "/data";
    private ZookeeperConfigurationClient zookeeperConfigurationClient;

    public ConfigWatcher(String hosts) throws IOException, InterruptedException {
        zookeeperConfigurationClient = new ZookeeperConfigurationClient();
        zookeeperConfigurationClient.connect(hosts);
    }

    public void displayConfig() throws KeeperException, InterruptedException {
        String value = zookeeperConfigurationClient.read(PATH, this);
        System.out.printf("Read %s is %s\n", PATH, value);
    }
    @Override
    public void process(WatchedEvent event) {
        /*如果有变化，就读取*/
        if (event.getType() == Event.EventType.NodeDataChanged) {
            try {
                displayConfig();
            } catch (KeeperException e) {

                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ConfigWatcher configWatcher = new ConfigWatcher("hd-even-01:2181,hd-even-02:2181,hd-even-03:2181");
        configWatcher.displayConfig();
        Thread.sleep(Long.MAX_VALUE);
    }
}
