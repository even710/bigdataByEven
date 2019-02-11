package com.even.api;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * des:
 * author: Even
 * create date:2019/2/11
 */
public class CountBolt extends BaseRichBolt {

    private Map<String, Integer> map = new HashMap<>();

    /*没有下一步了，不需要收集器*/
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
    }

    @Override
    public void execute(Tuple tuple) {
        /*分别对应前面的两个数值*/
        String split = tuple.getString(0);
        Integer sum = tuple.getInteger(1);
        if (map.containsKey(split)) {
            map.put(split, map.get(split) + sum);
        } else {
            map.put(split, sum);
        }

        System.out.println(Thread.currentThread().getId() + "单词为：" + split + "出现了" + map.get(split) + "次");
    }

    /*没有组件调用了，不需要起别名*/
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
