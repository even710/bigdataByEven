package com.even.order;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * des: 发生在Shuffle的第11步骤，按Key分组
 * author: Even
 * create date:2019/1/2
 */
public class OrderGroupingComparator extends WritableComparator {
    public OrderGroupingComparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean orderA = (OrderBean) a;
        OrderBean orderB = (OrderBean) b;
        /*id不同代表不是同一个对象，大的往下排，相同的不用动*/
        return Integer.compare(orderA.getOrder_id(), orderB.getOrder_id());
    }
}
