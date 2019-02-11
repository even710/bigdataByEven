package com.even.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * des: 在WordCount中，需要两个Bolt，一个Bolt的作用是把数据进行拆分，另一个Bolt的作用是把拆分后的数据进行合并加总
 * author: Even
 * create date:2019/2/11
 */
public class SplitBolt extends BaseRichBolt {


    /*改变收集器的作用域*/
    private OutputCollector collector;

    /**
     * 初始化
     *
     * @param map
     * @param topologyContext
     * @param collector
     */
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 业务逻辑，执行拆分的操作
     *
     * @param tuple
     */
    @Override
    public void execute(Tuple tuple) {
        /*1，获取数据*/
        String value = tuple.getStringByField("word");
        /*2，拆分数据*/
        String[] split = value.split(" ");
        /*3，<单词,1>发送到下一个bolt*/
        for (String word : split) {
            collector.emit(new Values(word, 1));
        }
    }

    /**
     * 给Bolt取个别名
     *
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        /*上面发送了两个数据，所以此处有两个字段，分别代表上面的两个数值*/
        declarer.declare(new Fields("split","sum"));
    }
}
