package com.even.order;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * des: 订单id按正序排序，价格按高到低排序，输出三个文件，每个文件只有一条数据。
 * author: Even
 * create date:2019/1/2
 */
public class OrderDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /*1，定义Job*/
        Job job = Job.getInstance(new Configuration());

        /*2，设置驱动类*/
        job.setJarByClass(OrderDriver.class);

        /*3，设置Mapper类和Reducer类*/
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        /*4，设置Map阶段输出结果的键类型和值类型*/
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        /*5，设置Reduce阶段输出结果的键类型和值类型*/
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        /*6，设置分区规则以及ReduceTask数*/
        job.setPartitionerClass(OrderPartitioner.class);
        job.setNumReduceTasks(3);

        /*7，分组，发生在shuffle的按键分组步骤*/
        job.setGroupingComparatorClass(OrderGroupingComparator.class);

        /*8，输入输出路径*/
        FileInputFormat.setInputPaths(job, new Path("E:\\ideaworkspace\\bigdataByEven\\evenhadoop\\src\\main\\resources\\file\\orderin"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\ideaworkspace\\bigdataByEven\\evenhadoop\\src\\main\\resources\\file\\orderout"));

        /*9，提交等待*/
        System.out.println(job.waitForCompletion(true) ? 1 : 0);
    }
}
