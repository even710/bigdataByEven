package com.even.kafka;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.util.Properties;

/**
 * des: 启动程序，创建两个主题t1和t2，生产者往t1发送消息，消费者订阅t2主题，收到处理过后的数据
 * author: Even
 * create date:2019/2/4
 */
public class Application {
    public static void main(String[] args) {
        /*1，定义主题发送到另外一个主题中进行数据清洗*/
        String fromTopic = "t1";
        String toTopic = "t2";

        /*2，设置属性*/
        Properties properties = new Properties();
        /*指定应用id*/
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "logProcessor");
        /*指定Kafka集群*/
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "hd-even-01:9092,hd-even-02:9092,hd-even-03:9092");

        /*3，实例对象,kafka-stream的配置*/
        StreamsConfig config = new StreamsConfig(properties);

        /*4，流计算，需要设置一个拓扑结构*/
        Topology builder = new Topology();

        /*5，定义kafka组件的数据来源，数据计算类，parentName是前一个add方法的Name*/
        builder.addSource("Source", fromTopic).addProcessor("Processor", new ProcessorSupplier<byte[], byte[]>() {
            @Override
            public Processor<byte[], byte[]> get() {
                return new LogProcessor();
            }
        }, "Source").addSink("Sink", toTopic, "Processor");

        /*6，实例化KafkaStreams*/
        KafkaStreams streams = new KafkaStreams(builder, properties);
        streams.start();
    }
}
