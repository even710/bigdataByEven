package com.even.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

/**
 * des: 生产者添加拦截器
 * author: Even
 * create date:2019/2/4
 */
public class InterceptorProducer {
    public static void main(String[] args) {
        /*1，配置生产者属性（指定多个属性）*/
        Properties properties = new Properties();

        /*参数配置，下面的参数都可用使用ProducerConfig中的静态变量来替代*/
        /*kafka节点地址*/
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hd-even-01:9092");
        /*发送消息是否等待应答，即确认是否发送成功*/
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
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

        /*拦截器，作用是用于过滤从生产者到消费者的消息*/
        ArrayList<String> list = new ArrayList<>();
        list.add("com.even.kafka.TimeInterceptor");
        /*interceptor.classes*/
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, list);

        /*2，实例化producer*/
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        /*3，发送消息*/
        for (int i = 0; i < 99; i++) {
            producer.send(new ProducerRecord<String, String>("testKafka", "test successful" + i));
        }
        /*4，释放资源*/
        producer.close();
    }
}
