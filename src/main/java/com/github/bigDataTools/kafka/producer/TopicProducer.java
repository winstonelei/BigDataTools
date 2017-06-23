package com.github.bigDataTools.kafka.producer;

import com.github.bigDataTools.kafka.model.DefaultMessage;
import com.github.bigDataTools.kafka.producer.handler.ProducerEventHandler;

/**
 * Created by winstone on 2017/6/12.
 */
public interface TopicProducer {


    boolean publish(final String topic, final DefaultMessage message, boolean asynSend);


    void close();


    void addEventHandler(ProducerEventHandler handler);
}
