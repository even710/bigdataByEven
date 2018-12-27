package com.even.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * des:
 * author: Even
 * create date:2018/12/27
 */
public class FlowPartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        String phoneHead = text.toString().substring(0, 3);
        int partitioner = 4;
        switch (phoneHead) {
            case "135":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return partitioner;
        }
    }
}
