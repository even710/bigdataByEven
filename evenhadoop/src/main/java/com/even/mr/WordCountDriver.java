package com.even.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Project Name: Web_App
 * Des:
 * Created by Even on 2018/12/13
 */
public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /*1,获取Job信息*/
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        /*2，获取driver包，此处即WorkCountDriver类*/
        job.setJarByClass(WordCountDriver.class);
        /*3，设置mapper和reduce*/
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReduce.class);
        /*4，设置Mapper阶段数据输出类型*/
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        /*5，设置reduce阶段数据输出类型*/
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        /*指定InputFormat的方式*/
        job.setInputFormatClass(CombineTextInputFormat.class);
        /*指定最大值*/
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        /*指定最小值*/
        CombineTextInputFormat.setMinInputSplitSize(job, 3145728);
        /*6，设置输入存在的路径与处理后的结果路径*/
        FileInputFormat.setInputPaths(job, new Path("/in"));
        FileOutputFormat.setOutputPath(job, new Path("/out"));
        /*7，提交任务*/
        boolean b = job.waitForCompletion(true);
        System.out.println(b ? 1 : 0);
    }
}
