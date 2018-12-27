package com.even.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * des:
 * author: Even
 * create date:2018/12/27
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*1，获取数据*/
        String line = value.toString();
        /*2，切割数据*/
        String[] words = line.split("\t");
        /*3，封闭数据*/
        String phoneNum = words[1];
        long upFlow = Long.parseLong(words[words.length - 3]);
        long downFlow = Long.parseLong(words[words.length - 2]);
        /*4，传输数据*/
        context.write(new Text(phoneNum), new FlowBean(upFlow, downFlow));
    }
}
