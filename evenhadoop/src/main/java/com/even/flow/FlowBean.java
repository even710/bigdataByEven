package com.even.flow;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * des:
 * author: Even
 * create date:2018/12/27
 */
public class FlowBean implements WritableComparable<FlowBean> {
    private long upFLow;
    private long downFlow;
    private long flowSum;

    public FlowBean() {
    }

    public FlowBean(long upFLow, long downFlow) {
        this.upFLow = upFLow;
        this.downFlow = downFlow;
        this.flowSum = upFLow + downFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFLow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(flowSum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFLow = dataInput.readLong();
        downFlow = dataInput.readLong();
        flowSum = dataInput.readLong();
    }

    long getUpFLow() {
        return upFLow;
    }

    public void setUpFLow(long upFLow) {
        this.upFLow = upFLow;
    }

    long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getFlowSum() {
        return flowSum;
    }

    public void setFlowSum(long flowSum) {
        this.flowSum = flowSum;
    }

    @Override
    public String toString() {
        return upFLow + "\t" + downFlow + "\t" + flowSum;
    }


    @Override
    public int compareTo(FlowBean o) {
        return this.flowSum > o.getFlowSum() ? -1 : 1;
    }
}
