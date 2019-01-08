package com.even.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Project Name: Web_App
 * Des:
 * Created by Even on 2018/12/13
 */
public class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        /*1,定义sum*/
        int sum = 0;
        /*2，累加求各*/
        for (IntWritable count : values)
            sum += count.get();
        /*3，结果输出*/
        context.write(key, new IntWritable(sum));

    }
}


