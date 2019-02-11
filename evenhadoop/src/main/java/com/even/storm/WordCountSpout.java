package com.even.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * des:
 * Storm的“水龙头”，即源
 * 两种方法：
 * 实现接口：IRichSpout IRichBolt
 * 继承抽象类：BaseRichSpout BaseRichBolt
 * author: Even
 * create date:2019/2/11
 */
public class WordCountSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    /**
     * 接收外部的数据源，创建收集器
     *
     * @param map
     * @param topologyContext
     * @param collector       收集器
     */
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
    }


    //数据传输的最小单元是Tuple，元组。此处作用是发送元组到下一个Bolt，即发送数据等操作
    @Override
    public void nextTuple() {
        /*1，发送数据*/
        collector.emit(new Values("i am even"));
        /*2，设置延迟，方便查看*/
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 声明描述，相当于给此Spout取个别名
     *
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }
}
