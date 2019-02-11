package com.even.kafka;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * des: 对数据进行一些简单的清洗，和Application类配合使用
 * author: Even
 * create date:2019/2/4
 */
public class LogProcessor implements Processor<byte[], byte[]> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    @Override
    public void process(byte[] key, byte[] value) {
        String message = new String(value);
        if (message.contains("-")) {
            String[] strings = message.split("-");
            message = strings[1];
        }
        context.forward(key, message.getBytes());

    }

    @Override
    public void close() {

    }
}
