package com.even.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.StringWriter;

/**
 * Project Name: Web_App
 * Des:WordCount的Mapper
 * 1，需要继承Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
 * KEYIN：输入的Key的类型，默认为LongWritable，代表偏移量，可以理解为行号。
 * VALUEIN：输入的数据的类型，字符串类型对应Hadoop的Text
 * KEYOUT：输出的数据的键的类型，这里是Text
 * VALUEOUT：输出的数据的类型，因为是求单词出现的次数，用int，对应Hadoop是IntWritable
 * 2，重写map方法，其中key是输入的数据的键，value是输入的数据，context代表上下文，用于传递数据
 * Created by Even on 2018/12/12
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * 计算每个单词出现的次数
     *
     * @param key     输入数据的键
     * @param value   输入数据
     * @param context 上下文
     * @throws IOException          IO异常
     * @throws InterruptedException 线程重复的异常
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*1，读取数据*/
        String line = value.toString();
        /*2，切分数据*/
        String[] words = line.split(" {2}");
        /*3，循环写到*/
        for (String word : words) {
            /*4，输出到reduce阶段*/
            context.write(new Text(word), new IntWritable(1));
        }

    }
}
