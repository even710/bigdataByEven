package com.even.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * des:
 * author: Even
 * create date:2019/2/4
 */
public class Consumer {
    public static void main(String[] args) {
        /*1,指定配置*/
        Properties properties = new Properties();
        /*kafka节点地址*/
        properties.put("bootstrap.servers", "hd-even-01:9092");
        /*配置组id*/
        properties.put("group.id", "g1");
        /*配置是否自动确认offset，当消费消息时，可以实时确认当前偏移量*/
        properties.put("enable.auto.commit.", "true");
        /*反序列化*/
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        /*2，实例化消费者*/
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        /*3，释放资源，线程安全，启动一个线程，当没有消息时，关闭consumer?此处有问题*/
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                consumer.close();
            }
        }));
        /*4，订阅主题*/
        consumer.subscribe(Collections.singletonList("testKafka"));
        /*5,获取消息*/
        while (true) {
            /*拉消息*/
            ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
            /*遍历消息*/
            for (ConsumerRecord record : consumerRecords) {
                System.out.println(record.topic() + "----------" + record.value());
            }

        }
    }
}
