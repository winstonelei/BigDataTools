package com.github.bigDataTools.kafka.producer;

import com.github.bigDataTools.kafka.model.DefaultMessage;
import com.github.bigDataTools.kafka.producer.handler.ProducerEventHandler;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Created by winstone on 2017/6/12.
 */
public class DefaultTopicProducer implements  TopicProducer {

    private final Logger LOG = LoggerFactory.getLogger(DefaultTopicProducer.class);

    private  KafkaProducer<String, Object> kafkaProducer = null;

    private boolean asyncSend = true;

    private List<ProducerEventHandler> eventHandlders = new ArrayList<>();


    public DefaultTopicProducer(Properties properties,boolean sendFlag){

        this.kafkaProducer = new KafkaProducer<String, Object>(properties);
        this.asyncSend = sendFlag;

    }

    @Override
    public boolean publish(final String topic, final DefaultMessage message, boolean flag) {

        Validate.notNull(topic, "Topic is required");

        Validate.notNull(message, "Message is required");

        if(flag){
            try {
                doAsynSend(topic, message.getMsgId(),message);
            } catch (Exception e) {
                LOG.error("kafka_send_fail,topic="+topic+",messageId="+message.getMsgId(),e);
                throw new RuntimeException(e);
            }
            return true;
        }else{
            return doSyncSend(topic, message.getMsgId(), message);
        }

    }

    /**
     * 异步发送消息
     * @param topic
     * @param msgId
     * @param message
     */
    private void doAsynSend(final String topic, final String msgId, final DefaultMessage message) {
        this.kafkaProducer.send(new ProducerRecord<String, Object>(topic, msgId,message.isSendBodyOnly() ? message.getBody() : message), new Callback() {

            @Override
            public void onCompletion(RecordMetadata metadata, Exception ex) {
                if (ex != null) {
                    for (ProducerEventHandler handler : eventHandlders) {
                        try {
                            handler.onError(topic, message, true);
                        } catch (Exception e) {}
                    }
                    LOG.error("kafka_send_fail,topic="+topic+",messageId="+msgId,ex);
                } else {
                    for (ProducerEventHandler handler : eventHandlders) {
                        try {handler.onSuccessed(topic, metadata);} catch (Exception e) {}
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("kafka_send_success,topic=" + topic + ", messageId=" + msgId + ", partition=" + metadata.partition() + ", offset=" + metadata.offset());
                    }
                }
            }
        });
    }


    /**
     * 同步发送消息
     * @param topicName
     * @param messageKey
     * @param message
     * @return
     */
    private boolean doSyncSend(String topicName, String messageKey,DefaultMessage message){
        try {
            Future<RecordMetadata> future = kafkaProducer.send(new ProducerRecord<String, Object>(topicName, messageKey,message.isSendBodyOnly() ? message.getBody() : message));
            RecordMetadata metadata = future.get();
            for (ProducerEventHandler handler : eventHandlders) {
                try {handler.onSuccessed(topicName, metadata);} catch (Exception e) {}
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("kafka_send_success,topic=" + topicName + ", messageId=" + messageKey + ", partition=" + metadata.partition() + ", offset=" + metadata.offset());
            }
            return true;
        } catch (Exception ex) {
            LOG.error("kafka_send_fail,topic="+topicName+",messageId="+messageKey,ex);
            //同步发送直接抛异常
            throw new RuntimeException(ex);
        }
    }
    @Override
    public void close() {
        this.kafkaProducer.close();
        for (ProducerEventHandler handler : eventHandlders) {
            try {
                handler.close();
            } catch (Exception e) {
                LOG.error(e.getMessage());
            }
        }
    }

    @Override
    public void addEventHandler(ProducerEventHandler handler) {
      this.eventHandlders.add(handler);
    }
}
