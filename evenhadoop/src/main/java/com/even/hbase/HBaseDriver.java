package com.even.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * des:HBase使用MR需要实现Tool
 * author: Even
 * create date:2019/1/30
 */
public class HBaseDriver implements Tool {
    private Configuration conf;


    @Override
    public int run(String[] strings) throws Exception {
        /*1，创建任务*/
        Job job = Job.getInstance(conf);
        /*2，指定运行的类*/
        job.setJarByClass(HBaseDriver.class);
        /*3，创建scan，以查询源table1表的数据*/
        Scan scan = new Scan();

        /*4,配置Mapper*/
        TableMapReduceUtil.initTableMapperJob("table1", scan, HBaseMapper.class, ImmutableBytesWritable.class, Put.class, job);
        /*5，配置Reducer*/
        TableMapReduceUtil.initTableReducerJob("table2", HBaseReducer.class, job);

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
            status = ToolRunner.run(new HBaseDriver(), args);
            System.out.println("运行成功！");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.exit(status);
        }
    }
}
