package com.even.order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * des: 拆分数据
 * author: Even
 * create date:2019/1/2
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        int order_id = Integer.parseInt(fields[0]);
        double price = Double.parseDouble(fields[2]);
        context.write(new OrderBean(order_id, price), NullWritable.get());
    }
}
