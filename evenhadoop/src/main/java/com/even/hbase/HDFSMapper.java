package com.even.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * des:因为是从hdfs中获取数据，相当于是从文件中获取，跟以前写的MapReduce程序一样的输入，但是输出是要HBase的类型
 * author: Even
 * create date:2019/1/30
 */
public class HDFSMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*1，获取数据*/
        String line = value.toString();
        /*2，切分数据*/
        String[] fields = line.split("\t");
        /*3，封装数据*/
        String rowKey = fields[0];
        String name = fields[1];
        String des = fields[2];
        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(Bytes.toBytes(rowKey));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"), Bytes.toBytes(name));
        put.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("des"), Bytes.toBytes(des));

        /*4，输出数据*/
        context.write(immutableBytesWritable, put);


    }
}
