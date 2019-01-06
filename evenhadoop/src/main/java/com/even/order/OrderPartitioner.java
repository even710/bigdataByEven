package com.even.order;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * des: Map从环形缓冲区溢写后发生分区，shuffle第3步
 * author: Even
 * create date:2019/1/2
 */
public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {
        /*i代表job.setNumReduceTasks方法设置的个数*/
        /*取3的模，会输出三个分区*/
        return (orderBean.getOrder_id() & Integer.MAX_VALUE) % i;
    }
}
