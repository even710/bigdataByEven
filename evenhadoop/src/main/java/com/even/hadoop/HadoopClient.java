package com.even.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;


public class HadoopClient {

    /**
     * 从本地复制文件到Hadoop
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public void copyFileFromLocal() throws URISyntaxException, IOException, InterruptedException {
        /*1,从hadoop-common.jar中的core-default.xml读取默认值*/
        Configuration configuration = new Configuration();

        /*2，配置副本数*/
        configuration.set("dfs.replication", "2");

        /*3,配置块大小*/
        configuration.set("dfs.blocksize", "64m");

        /*4，构造客户端*/
        FileSystem fs = FileSystem.get(new URI("hdfs://hd-even-01:9000"), configuration, "root");

        /*5,复制文件*/
        fs.copyFromLocalFile(new Path("C:\\Users\\Administrator\\Desktop\\win10激活.txt"), new Path("/"));

        /*关闭流，报错winUtils,因为使用了linux的tar包，如果windows要使用，则需要编译好这个winUtils包才能使用*/
        fs.close();
    }

    /**
     * 从Hadoop下载文件到本地，下载需要配置Hadoop环境，并添加winutils到bin目录
     *
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    public void copyFileToLocal() throws URISyntaxException, IOException, InterruptedException {
        /*1,从hadoop-common.jar中的core-default.xml读取默认值*/
        Configuration configuration = new Configuration();

        /*2，配置副本数*/
        configuration.set("dfs.replication", "2");

        /*3,配置块大小*/
        configuration.set("dfs.blocksize", "64m");

        /*4，构造客户端*/
        FileSystem fs = FileSystem.get(new URI("hdfs://hd-even-01:9000"), configuration, "root");

        /*5,复制文件*/
        fs.copyToLocalFile(new Path("/win10激活.txt"), new Path("E:/"));

        /*关闭流，报错winUtils,因为使用了linux的tar包，如果windows要使用，则需要编译好这个winUtils包才能使用*/
        fs.close();
    }

    private FileSystem fileSystem = null;

    public static void main(String[] args) {
        HadoopClient hadoopClient = new HadoopClient();
        try {
            hadoopClient.copyFileToLocal();
        } catch (URISyntaxException | IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    //    FileSystem fileSystem =
    public void init() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();

        configuration.set("dfs.replication", "2");
        configuration.set("dfs.blocksize", "64m");
        fileSystem = FileSystem.get(new URI("hdfs://hd-even-01:9000"), configuration, "root");
    }

    public void hdfsMkdir() throws IOException {
        /*调用创建文件夹方法*/
        fileSystem.mkdirs(new Path("/even1"));
        /*关闭方法*/
        fileSystem.close();
    }

    /**
     * 移动文件/修改文件名
     */
    public void hdfsRename() throws IOException {
        fileSystem.rename(new Path(""), new Path(""));
        fileSystem.close();
    }

    /**
     * 删除文件/文件夹
     *
     * @throws IOException
     */
    public void hdfsRm() throws IOException {
        fileSystem.delete(new Path(""));
        /*第二个参数表示递归删除*/
        fileSystem.delete(new Path(""), true);

        fileSystem.close();
    }

    /**
     * 查看hdfs指定目录的信息
     *
     * @throws IOException
     */
    public void hdfsLs() throws IOException {
        /*调用方法返回远程迭代器 */
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), false);
        while (listFiles.hasNext()) {
            LocatedFileStatus locatedFileStatus = listFiles.next();

            System.out.println("文件路径：" + locatedFileStatus.getPath());
            System.out.println("块大小：" + locatedFileStatus.getBlockSize());
            System.out.println("文件长度：" + locatedFileStatus.getLen());
            System.out.println("块信息：" + Arrays.toString(locatedFileStatus.getBlockLocations()));
            System.out.println("副本数量：" + locatedFileStatus.getReplication());
        }

        fileSystem.close();
    }
}
