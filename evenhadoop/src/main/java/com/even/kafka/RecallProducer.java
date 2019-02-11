package com.even.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * des: 生产者回调+分区，默认分区是0，consumer需要监听和生产者发送消息时的分区一样才会接收到消息，回调只保证消息发送成功，但不保证是否被消费者消费
 * 回调可以类比RabbitMQ中的ReturnConfirm
 * author: Even
 * create date:2019/2/4
 */
public class RecallProducer {
    public static void main(String[] args) {
        /*1，配置生产者属性（指定多个属性）*/
        Properties properties = new Properties();

        /*参数配置*/
        /*kafka节点地址*/
        properties.put("bootstrap.servers", "hd-even-01:9092");
        /*发送消息是否等待应答，即确认是否发送成功*/
        properties.put("acks", "all");
        /*消息发送失败是否重试*/
        properties.put("retries", "0");
        /*指定批量处理的大小*/
        properties.put("batch.size", "2048");
        /*配置批量处理数据的延迟*/
        properties.put("linger.ms", "5");
        /*配置内存缓冲大小*/
        properties.put("buffer.memory", "4194304");
        /*配置发送前必须序列化*/
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        /*指定分区的实现类*/
//        properties.put("partitioner.class","com.even.kafka.KafkaPartition");

        /*2，实例化producer*/
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        /*3，发送消息*/
        for (int i = 0; i < 99; i++) {
            /*添加回调参数*/
            producer.send(new ProducerRecord<String, String>("testKafka", "test partition" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    /*recordMetadata是记录的元数据信息，记录了当前数据的偏移量，分区等信息*/
                    if (null != recordMetadata) {
                        System.out.println("主题是：" + recordMetadata.topic());
                        System.out.println("偏移量：" + recordMetadata.offset());
                        System.out.println("分区是：" + recordMetadata.partition());
                    }
                }
            });
        }
        /*4，释放资源*/
        producer.close();
    }
}
