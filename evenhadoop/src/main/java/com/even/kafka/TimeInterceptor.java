package com.even.kafka;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * des: 拦截器实现，在应用中，往往在发送消息时，会出现一些垃圾数据，因此，需要使用一个拦截器来过滤这些消息
 * author: Even
 * create date:2019/2/4
 */
public class TimeInterceptor implements ProducerInterceptor<String, String> {
    /*业务逻辑，给消息添加一个时间戳*/
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {
        return new ProducerRecord<String, String>(producerRecord.topic(), producerRecord.partition(), producerRecord.key(), System.currentTimeMillis() + "-" + producerRecord.value());
    }

    /*发送失败时调用*/
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
