package com.github.bigDataTools.kafka.producer;

import com.github.bigDataTools.kafka.model.DefaultMessage;
import com.github.bigDataTools.kafka.producer.handler.ErrorDelayRetryHandler;
import com.github.bigDataTools.kafka.producer.handler.SendCounterHandler;
import com.github.bigDataTools.util.NodeNameHolder;
import com.github.bigDataTools.util.serializable.KyroMessageSerializer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Serializable;
import java.util.Properties;
import java.util.ResourceBundle;

/**
 * Created by winstone on 2017/6/12.
 */
public class KafkaProducerManager {

    private Properties configs;

    //默认是否异步发送
    private boolean defaultAsynSend = true;

    private String producerGroup;

    private String monitorZkServers;

    //延迟重试次数
    private int delayRetries = 3;

    //环境路由
    private String routeEnv;

    private TopicProducer producer;

    protected final ResourceBundle rb ;

    private static KafkaProducerManager kafkaProducerManager = new KafkaProducerManager();

    public static KafkaProducerManager getInstance(){
         return kafkaProducerManager;
    }

    private KafkaProducerManager(){

        configs = new Properties();

        if(!configs.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)){
            configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); // key serializer
        }

        if(!configs.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)){
            configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KyroMessageSerializer.class.getName());
        }

        if(!configs.containsKey(ProducerConfig.PARTITIONER_CLASS_CONFIG)){
            configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class.getName());
        }

        //默认重试一次
        if(!configs.containsKey(ProducerConfig.RETRIES_CONFIG)){
            configs.put(ProducerConfig.RETRIES_CONFIG, "1");
        }

        if(!configs.containsKey(ProducerConfig.COMPRESSION_TYPE_CONFIG)){
            configs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        }

        if(!configs.containsKey("client.id")){
            configs.put("client.id", (producerGroup == null ? "" : "_"+producerGroup) + NodeNameHolder.getNodeId());
        }


        rb = ResourceBundle.getBundle("commons");
        if (rb == null) {
            throw new RuntimeException("kafka.properties配置文件不存在");
        }

        configs.put("bootstrap.servers", rb.getString("kafkaServers"));
        configs.put("acks", "1");
        configs.put("retries", "1");


        monitorZkServers = rb.getString("monitorZkServers");
        producerGroup = rb.getString("producerGroup");
        this.producer = new DefaultTopicProducer(configs,defaultAsynSend);


        //hanlder
        if(StringUtils.isNotBlank(monitorZkServers)){
            Validate.notBlank(producerGroup,"enable producer monitor property[producerGroup] is required");
            this.producer.addEventHandler(new SendCounterHandler(producerGroup,monitorZkServers));
        }
        if(delayRetries > 0){
            this.producer.addEventHandler(new ErrorDelayRetryHandler(producerGroup,producer, delayRetries));
        }


    }



    public boolean publish(final String topicName, final DefaultMessage message) {
        return publish(topicName, message, defaultAsynSend);
    }


    public boolean publish(String topicName, DefaultMessage message,boolean asynSend){
        if(routeEnv != null)topicName = routeEnv + "." + topicName;
        return producer.publish(topicName, message,asynSend);
    }



    public boolean publishNoWrapperMessage(String topicName, Serializable message, boolean asynSend) {
        DefaultMessage defaultMessage = new DefaultMessage(message).sendBodyOnly(true);
        if(routeEnv != null)topicName = routeEnv + "." + topicName;
        return producer.publish(topicName, defaultMessage,asynSend);
    }



}
