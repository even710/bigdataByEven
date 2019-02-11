package com.even.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * des: Mapper阶段跟处理正常的文件一样的设置，只是Reducer阶段是需要输出到HBase表，因此需要用到HBase的API
 * author: Even
 * create date:2019/1/31
 */
public class HDFSDriver implements Tool {
    private Configuration conf;

    @Override
    public int run(String[] strings) throws Exception {
        /*1，创建任务*/
        Job job = Job.getInstance(conf);
        /*2，指定主类*/
        job.setJarByClass(HDFSDriver.class);
        job.setMapperClass(HDFSMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        TableMapReduceUtil.initTableReducerJob("table2", HDFSReducer.class, job);

        FileInputFormat.setInputPaths(job, new Path("/hdfs2hbase"));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = HBaseConfiguration.create(configuration);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) {
        int status = 0;
        try {
            status = ToolRunner.run(new HDFSDriver(), args);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.exit(status);
        }
    }
}
