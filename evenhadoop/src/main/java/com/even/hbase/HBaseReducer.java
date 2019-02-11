package com.even.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * des: 把数据遍历输出，因为后面没有操作了，所以输出类型为NullWritable
 * author: Even
 * create date:2019/1/30
 */
public class HBaseReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        for (Put p : values) {
            context.write(NullWritable.get(), p);
        }
    }
}
