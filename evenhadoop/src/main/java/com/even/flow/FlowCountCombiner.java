package com.even.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * des:
 * author: Even
 * create date:2018/12/27
 */
public class FlowCountCombiner extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long upFlow_sum = 0;
        long downFlow_sum = 0;

        for (FlowBean flowBean : values) {
            upFlow_sum += flowBean.getUpFLow();
            downFlow_sum += flowBean.getDownFlow();
        }
        context.write(key, new FlowBean(upFlow_sum, downFlow_sum));
    }
}
