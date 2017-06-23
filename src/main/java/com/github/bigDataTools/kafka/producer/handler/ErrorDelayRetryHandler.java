package com.github.bigDataTools.kafka.producer.handler;

import com.github.bigDataTools.kafka.model.DefaultMessage;
import com.github.bigDataTools.kafka.producer.TopicProducer;
import com.github.bigDataTools.util.StandardThreadExecutor;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.*;

/**
 * Created by winstone on 2017/6/12.
 */
public class ErrorDelayRetryHandler implements ProducerEventHandler {

    private static  final Logger LOG = LoggerFactory.getLogger(ErrorDelayRetryHandler.class);

    private TopicProducer  topicProducer;

    private int retries;

    private final PriorityBlockingQueue<PrioryTask> taskQueue = new PriorityBlockingQueue<PrioryTask>();

    private List<String> messageIdsInQueue = new Vector<>();

    private ExecutorService executor;

    public ErrorDelayRetryHandler(String producerGroup, TopicProducer producer, int retries){

        this.topicProducer = producer;

        this.retries = retries;

        executor = Executors.newFixedThreadPool(1 , new StandardThreadExecutor.StandardThreadFactory("ErrorMessageProcessor"));

        executor.submit(new Runnable() {
            @Override
            public void run() {
                    long currentTimeMillis = System.currentTimeMillis();
                    while(true){
                        try {
                            PrioryTask task = taskQueue.take();
                            //空任务跳出循环
                            if(task.message == null)break;
                            if(task.nextFireTime < currentTimeMillis){
                                //重新放回去
                                taskQueue.add(task);
                                TimeUnit.MILLISECONDS.sleep(100);
                                continue;
                            }
                            task.run();
                        } catch (Exception e) {}
                    }
                }
        });

    }

    @Override
    public void onSuccessed(String topicName, RecordMetadata metadata) {

    }

    @Override
    public void onError(String topicName, DefaultMessage message, boolean isAsynSend) {
        if(isAsynSend == false){
            return;
        }
        //在重试队列不处理
        if(messageIdsInQueue.contains(message.getMsgId()))return;
        //
        taskQueue.add(new PrioryTask(topicName,message));
        messageIdsInQueue.add(message.getMsgId());
    }

    @Override
    public void close() throws IOException {

    }


    class PrioryTask implements Runnable,Comparable<PrioryTask>{

        final String topicName;
        final DefaultMessage message;

        int retryCount = 0;
        long nextFireTime;


        public PrioryTask(String topicName,DefaultMessage message){
            this(topicName,message,System.currentTimeMillis());
        }

        public PrioryTask(String topicName,DefaultMessage message,long nextFireTime) {
            super();
            this.topicName = topicName;
            this.message = message;
            this.nextFireTime = nextFireTime;
        }


        @Override
        public int compareTo(PrioryTask o) {
            return (int) (this.nextFireTime - o.nextFireTime);
        }

        @Override
        public String toString() {
            return "PrioryTask [message=" + message.getMsgId() + ", retryCount="
                    + retryCount + ", nextFireTime=" + nextFireTime + "]";
        }


        @Override
        public void run() {
            try {
                LOG.debug("begin re process message:"+this.toString());
                Object sendContent = message.isSendBodyOnly() ? message.getBody() : message;
                topicProducer.publish(topicName,message, true);
                        //.send(new ProducerRecord<String, Object>(topicName, message.getMsgId(),sendContent));
                //处理成功移除
                messageIdsInQueue.remove(message.getMsgId());
            } catch (Exception e) {
                LOG.warn("retry mssageId[{}] error",message.getMsgId(),e);
                retry();
            }
        }

        private void retry(){
            if(retryCount == retries){
                return;
            }
            nextFireTime = nextFireTime + retryCount * 30 * 1000;
            //重新放入任务队列
            taskQueue.add(this);
            LOG.debug("re submit mssageId[{}] task to queue,next fireTime:",this.message.getMsgId(),nextFireTime);
            retryCount++;
        }

    }
}