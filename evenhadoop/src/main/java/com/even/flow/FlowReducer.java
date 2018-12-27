package com.even.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        /*1，汇总流量*/
        long upFlow_sum = 0;
        long downFlow_sum = 0;
        /*2，累加*/
        for (FlowBean flowBean : values) {
            upFlow_sum += flowBean.getUpFLow();
            downFlow_sum += flowBean.getDownFlow();
        }
        /*3，传输数据*/
        context.write(key, new FlowBean(upFlow_sum, downFlow_sum));
    }
}
