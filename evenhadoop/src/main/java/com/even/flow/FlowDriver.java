package com.even.flow;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * des:
 * author: Even
 * create date:2018/12/27
 */
public class FlowDriver {
    public static void main(String[] args) throws IOException {
        /*1，定义Job任务*/
        Job job = Job.getInstance();
        /*2，指定驱动类*/
        job.setJarByClass(FlowDriver.class);
        /*3，指定Mapper和Reducer类*/
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        /*4，指定Mapper输出键和值类型*/
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        /*5，指定Reducer输出键和值类型*/
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        /*设置InputFormat的方式*/
        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        CombineTextInputFormat.setMinInputSplitSize(job, 3145728);
        /*设置分区*/
        job.setPartitionerClass(FlowPartitioner.class);
        job.setNumReduceTasks(5);
        job.setCombinerClass(FlowCountCombiner.class);
//        job.setCombinerClass(FlowCountCombiner.class);
        /*6，指定输入输出路径*/
        FileInputFormat.setInputPaths(job, new Path(System.getProperty("user.dir") + "/evenhadoop/src/main/resources/file/in"));
        FileOutputFormat.setOutputPath(job, new Path(System.getProperty("user.dir") + "/evenhadoop/src/main/resources/file/out"));
        /*7，提交任务*/
        try {
            boolean b = job.waitForCompletion(true);
            System.out.println(b ? 1 : 0);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
