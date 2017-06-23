package com.github.bigDataTools.kafka.producer.handler;

import com.github.bigDataTools.kafka.model.DefaultMessage;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.Closeable;

/**
 * Created by winstone on 2017/6/12.
 */
public interface ProducerEventHandler extends Closeable {

    public void onSuccessed(String topicName, RecordMetadata metadata);

    public void onError(String topicName, DefaultMessage message, boolean isAsynSend);

}
