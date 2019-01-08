package com.even.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Project Name: Web_App
 * Des:
 * Created by Even on 2018/11/20
 */
public class HadoopClient {

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        /*1，添加配置*/
        Configuration configuration = new Configuration();

        /*设置副本数*/
        configuration.set("dfs.replication", "2");
        /*设置文件块大小，当上传的文件大于这个值时，会被切块*/
        configuration.set("dfs.blocksize", "64m");
        /*2，连接文件系统*/
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.11.136:9000"), configuration, "root");
        /*3，发送文件*/
        fs.copyFromLocalFile(new Path("D:\\log\\12-4-open.log"),
                new Path("/log"));
        fs.copyFromLocalFile(new Path("D:\\log\\12-04-close.log"),
                new Path("/log"));
        /*4，关闭文件流*/
        fs.close();
    }
}
