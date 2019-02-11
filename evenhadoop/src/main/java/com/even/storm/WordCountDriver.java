package com.even.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * des:
 * author: Even
 * create date:2019/2/11
 */
public class WordCountDriver {
    public static void main(String[] args) {
        /*1，创建topology对象*/
        TopologyBuilder builder = new TopologyBuilder();

        /*第三个参数代表并行度的意思，此处指定一个*/
        builder.setSpout("word", new WordCountSpout(), 2);
        /*第一种分组法*/
        /*.fieldsGrouping代表字段分组，第一个参数是上一行代码定义的"word"，第二个参数是上一行代码Spout中给数值取的别名，按照字段分组，最终输出会按时顺序输出，区别于用shuffle的随机分组*/
        builder.setBolt("split", new SplitBolt(), 4).fieldsGrouping("word", new Fields("word"));
        builder.setBolt("count", new CountBolt(), 1).fieldsGrouping("split", new Fields("split"));
        /*第二种分组法*/
        StormTopology topology = builder.createTopology();
        /*2,创建配置信息，继承了HashMap*/
        Config config = new Config();
        /*可设置worker数*/
//        config.setNumWorkers(10);
        /*本地启动*/
        LocalCluster cluster = new LocalCluster();
        /*提交拓扑，取个拓扑别名*/
        cluster.submitTopology("wordcount", config, topology);
    }
}
